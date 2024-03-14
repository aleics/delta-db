use std::collections::HashMap;
use std::path::Path;

use bimap::BiHashMap;
use roaring::RoaringBitmap;
use serde::{Deserialize, Serialize};
use sled::{Db, IVec};

use crate::DataItemId;
use crate::index::{Index, Indexable};

const FOLDER: &str = "./delta-db";
const DATA_FILE: &str = "data";
const INDICES_FILE: &str = "indices";
const POSITION_TO_ID_FILE: &str = "position_to_id";
const ID_TO_POSITION_FILE: &str = "id_to_position";

const ALL_ITEMS_KEY: &str = "__all";

pub enum EntityStorage<T> {
    Disk(DiskStorage),
    InMemory(InMemoryStorage<T>),
}

impl<T: Indexable + Serialize> EntityStorage<T> {
    pub fn carry(&mut self, data: Vec<T>) {
        match self {
            EntityStorage::Disk(disk) => disk.carry(data),
            EntityStorage::InMemory(in_memory) => in_memory.carry(data),
        }
    }

    pub fn clear(&mut self) {
        match self {
            EntityStorage::Disk(disk) => disk.clear(),
            EntityStorage::InMemory(in_memory) => in_memory.clear(),
        }
    }

    pub fn add(&mut self, item: T) {
        match self {
            EntityStorage::Disk(disk) => disk.add(item),
            EntityStorage::InMemory(in_memory) => in_memory.add(item),
        }
    }

    pub fn remove(&mut self, id: &DataItemId) {
        match self {
            EntityStorage::Disk(disk) => disk.remove(id),
            EntityStorage::InMemory(in_memory) => in_memory.remove(id),
        }
    }

    pub fn get_id_by_position(&self, position: &u32) -> Option<DataItemId> {
        match self {
            EntityStorage::Disk(disk) => disk.get_id_by_position(position),
            EntityStorage::InMemory(in_memory) => in_memory.get_id_by_position(position).copied(),
        }
    }

    pub fn get_position_by_id(&self, id: &DataItemId) -> Option<u32> {
        match self {
            EntityStorage::Disk(disk) => disk.get_position_by_id(id),
            EntityStorage::InMemory(in_memory) => in_memory.get_position_by_id(id).copied(),
        }
    }

    pub fn read_indices(&self, fields: &[String]) -> EntityIndices {
        match self {
            EntityStorage::Disk(disk) => disk.read_indices(fields),
            EntityStorage::InMemory(in_memory) => in_memory.read_indices(fields),
        }
    }

    pub fn read_all_indices(&self) -> EntityIndices {
        match self {
            EntityStorage::Disk(disk) => disk.read_all_indices(),
            EntityStorage::InMemory(in_memory) => in_memory.read_all_indices(),
        }
    }
}

impl<T: Indexable + Clone + for<'a> Deserialize<'a>> EntityStorage<T> {
    pub fn read(&self, id: &DataItemId) -> Option<T> {
        match self {
            EntityStorage::Disk(disk) => disk.read_by_id(id),
            EntityStorage::InMemory(in_memory) => in_memory.read_by_id(id),
        }
    }
}

pub struct StorageBuilder {
    kind: StorageKind,
}

impl StorageBuilder {
    pub fn disk() -> Self {
        StorageBuilder {
            kind: StorageKind::Disk,
        }
    }

    pub fn in_memory() -> Self {
        StorageBuilder {
            kind: StorageKind::InMemory,
        }
    }

    pub fn build<T: Indexable>(&self) -> EntityStorage<T> {
        match self.kind {
            StorageKind::Disk => EntityStorage::Disk(DiskStorage::init()),
            StorageKind::InMemory => EntityStorage::InMemory(InMemoryStorage::new()),
        }
    }
}

pub enum StorageKind {
    Disk,
    InMemory,
}

pub struct DiskStorage {
    data: Db,
    indices: Db,
    id_to_position: Db,
    position_to_id: Db,
}

impl DiskStorage {
    pub fn init() -> Self {
        let folder = Path::new(FOLDER);

        let indices = sled::open(folder.join(INDICES_FILE)).expect("Could not open indices file");
        let data = sled::open(folder.join(DATA_FILE)).expect("Could not open data file");
        let id_to_position = sled::open(folder.join(ID_TO_POSITION_FILE))
            .expect("Could not open position_to_id file");
        let position_to_id = sled::open(folder.join(POSITION_TO_ID_FILE))
            .expect("Could not open position_to_id file");

        DiskStorage {
            indices,
            position_to_id,
            id_to_position,
            data,
        }
    }

    pub fn carry<T, I>(&self, data: I)
        where
            I: IntoIterator<Item=T>,
            T: Indexable + Serialize,
    {
        self.clear();
        for item in data {
            self.add(item);
        }
    }

    pub(crate) fn clear(&self) {
        self.indices.clear().expect("Could not clear indices.");
        self.position_to_id
            .clear()
            .expect("Could not clear position to IDs mapping.");
        self.id_to_position
            .clear()
            .expect("Could not clear ID to positions mapping.");
        self.data.clear().expect("Could not clear data.");
    }

    pub(crate) fn add<T>(&self, item: T)
        where
            T: Indexable + Serialize,
    {
        // Read item ID and determine position
        let id = item.id();
        let id_bytes = id.to_le_bytes();

        let position = self
            .get_position_by_id(&id)
            .unwrap_or_else(|| self.id_to_position.len() as u32);

        let position_bytes = position.to_le_bytes();

        // Insert item in the data DB
        self.data.insert(id_bytes, serialize(&item)).unwrap();

        // Update indices with item's indexed values
        for index_value in item.index_values() {
            self.indices
                .update_and_fetch(&index_value.name, |value| {
                    let mut index: Index = value
                        .map(deserialize)
                        .unwrap_or_else(|| Index::from_type(&index_value.descriptor));

                    index.put(index_value.value.clone(), position);

                    Some(serialize(&index))
                })
                .unwrap();
        }

        self.indices
            .update_and_fetch(ALL_ITEMS_KEY, |value| {
                let mut all: RoaringBitmap = value.map(deserialize_bitmap).unwrap_or_default();
                all.insert(position);

                Some(serialize_bitmap(&all))
            })
            .unwrap();

        // Add item in the position to ID mapping
        self.position_to_id
            .insert(position_bytes, &id_bytes)
            .unwrap();
        self.id_to_position
            .insert(id_bytes, &position_bytes)
            .unwrap();
    }

    fn remove(&self, id: &DataItemId) {
        let id_bytes = id.to_le_bytes();

        if let Some(position) = self.get_position_by_id(id) {
            let position_bytes = position.to_le_bytes();

            // Remove item from data and ID to position mapping
            self.data.remove(id_bytes).unwrap();
            self.id_to_position.remove(id_bytes).unwrap();
            self.position_to_id.remove(position_bytes).unwrap();

            // Remove item from all indices
            for key in self.indices.iter().keys() {
                let key: String = key.map(|key| deserialize(key.as_ref()))
                    .expect("Could not read key of index while removing item");

                if key == ALL_ITEMS_KEY {
                    continue;
                }

                self.indices
                    .update_and_fetch(&key, |value| {
                        let mut index: Index = value.map(deserialize)?;
                        index.remove_item(*id as u32);
                        Some(serialize(&index))
                    })
                    .unwrap();
            }

            self.indices
                .update_and_fetch(ALL_ITEMS_KEY, |value| {
                    let mut all: RoaringBitmap = value.map(deserialize_bitmap)?;
                    all.remove(position);

                    Some(serialize_bitmap(&all))
                })
                .unwrap();
        }
    }

    fn get_id_by_position(&self, position: &u32) -> Option<DataItemId> {
        let position = position.to_le_bytes();
        let bytes = self
            .position_to_id
            .get(position)
            .expect("Could not read id by position")?
            .as_ref()
            .try_into()
            .expect("Could not transform byte slice into an array.");

        Some(DataItemId::from_le_bytes(bytes))
    }

    fn get_position_by_id(&self, id: &DataItemId) -> Option<u32> {
        let id = id.to_le_bytes();
        let bytes = self
            .id_to_position
            .get(id)
            .expect("Could not read position by id")?
            .as_ref()
            .try_into()
            .expect("Could not transform byte slice into an array.");

        Some(u32::from_le_bytes(bytes))
    }

    fn read_by_id<T>(&self, id: &DataItemId) -> Option<T>
        where
            T: Indexable + for<'a> Deserialize<'a>,
    {
        let id_bytes = id.to_le_bytes();

        self.data
            .get(id_bytes)
            .expect("Could not read item from DB")
            .map(|value| deserialize(value.as_ref()))
    }

    fn read_indices(&self, fields: &[String]) -> EntityIndices {
        let field_indices = fields
            .iter()
            .filter_map(|name| self.get_index(name).map(|index| (name.to_string(), index)))
            .collect();

        let all = self
            .indices
            .get(ALL_ITEMS_KEY)
            .expect("Could not read ALL items index from DB.")
            .map(|value| deserialize_bitmap(value.as_ref()))
            .expect("ALL items index is not present in DB");

        EntityIndices { field_indices, all }
    }

    fn read_all_indices(&self) -> EntityIndices {
        let field_indices = self
            .indices
            .iter()
            .map(|item| item.expect("Could not read index from DB."))
            .filter_map(|(key, value)| {
                let key = String::from_utf8(key.as_ref().to_vec())
                    .expect("Could not deserialize key while reading indices.");

                if key == ALL_ITEMS_KEY {
                    None
                } else {
                    let value = deserialize(value.as_ref());
                    Some((key, value))
                }
            })
            .collect();

        let all = self
            .indices
            .get(ALL_ITEMS_KEY)
            .expect("Could not read ALL items index from DB.")
            .map(|value| deserialize_bitmap(value.as_ref()))
            .expect("ALL items index is not present in DB");

        EntityIndices { field_indices, all }
    }

    fn get_index(&self, field: &str) -> Option<Index> {
        self.indices
            .get(field)
            .expect("Could not read index")
            .map(|value| deserialize(value.as_ref()))
    }
}

fn serialize<T: Serialize>(data: &T) -> IVec {
    let bytes = bincode::serialize(data).unwrap();
    IVec::from(bytes.as_slice())
}

fn deserialize<'a, T: Deserialize<'a>>(bytes: &'a [u8]) -> T {
    bincode::deserialize(bytes).unwrap()
}

fn serialize_bitmap(bitmap: &RoaringBitmap) -> IVec {
    let mut bytes = vec![];
    bitmap.serialize_into(&mut bytes).unwrap();
    IVec::from(bytes.as_slice())
}

fn deserialize_bitmap(bytes: &[u8]) -> RoaringBitmap {
    RoaringBitmap::deserialize_from(bytes).unwrap()
}

pub struct InMemoryStorage<T> {
    /// Indices available for the given associated data
    pub(crate) indices: EntityIndices,

    /// Mapping between position of a data item in the index and its ID
    position_id: BiHashMap<u32, DataItemId>,

    /// Data available in the storage associated by the ID
    pub(crate) data: HashMap<DataItemId, T>,
}

impl<T: Indexable> InMemoryStorage<T> {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn carry<I: IntoIterator<Item=T>>(&mut self, data: I) {
        self.clear();
        for item in data {
            self.add(item);
        }
    }

    pub(crate) fn clear(&mut self) {
        self.indices.all.clear();
        self.indices.field_indices.clear();
        self.position_id.clear();
        self.data.clear();
    }

    pub(crate) fn add(&mut self, item: T) {
        let id = item.id();

        let position = self
            .position_id
            .get_by_right(&id)
            .copied()
            .unwrap_or(self.position_id.len() as u32);

        for index_value in item.index_values() {
            // Create index for the key value
            let index = self
                .indices
                .field_indices
                .entry(index_value.name)
                .or_insert(Index::from_type(&index_value.descriptor));

            index.put(index_value.value, position);
        }
        self.indices.all.insert(position);

        // Associate index position to the field ID
        self.data.insert(id, item);
        self.position_id.insert(position, id);
    }

    pub(crate) fn remove(&mut self, id: &DataItemId) {
        if let Some((position, _)) = self.position_id.remove_by_right(id) {
            self.data.remove(id);

            // Remove item from indices
            for index in self.indices.field_indices.values_mut() {
                index.remove_item(position);
            }
            self.indices.all.remove(position);
        }
    }

    pub(crate) fn get_id_by_position(&self, position: &u32) -> Option<&DataItemId> {
        self.position_id.get_by_left(position)
    }

    pub(crate) fn get_position_by_id(&self, id: &DataItemId) -> Option<&u32> {
        self.position_id.get_by_right(id)
    }

    fn read_indices(&self, fields: &[String]) -> EntityIndices {
        let field_indices = fields
            .iter()
            .filter_map(|name| {
                self.indices
                    .field_indices
                    .get(name)
                    .cloned()
                    .map(|index| (name.to_string(), index))
            })
            .collect();

        EntityIndices {
            field_indices,
            all: self.indices.all.clone(),
        }
    }

    fn read_all_indices(&self) -> EntityIndices {
        let field_indices = self
            .indices
            .field_indices
            .iter()
            .map(|(name, index)| (name.to_string(), index.clone()))
            .collect();

        EntityIndices {
            field_indices,
            all: self.indices.all.clone(),
        }
    }
}

impl<T: Indexable + Clone> InMemoryStorage<T> {
    fn read_by_id(&self, id: &DataItemId) -> Option<T> {
        self.data.get(id).cloned()
    }
}

impl<T> Default for InMemoryStorage<T> {
    fn default() -> Self {
        InMemoryStorage {
            indices: Default::default(),
            position_id: Default::default(),
            data: Default::default(),
        }
    }
}

#[derive(Default)]
pub struct EntityIndices {
    /// Indices available associated by data's field name
    pub(crate) field_indices: HashMap<String, Index>,

    /// Bitmap including all items' positions
    pub(crate) all: RoaringBitmap,
}
