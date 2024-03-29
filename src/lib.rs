#[cfg(feature = "test-fixtures")]
pub mod fixtures;
pub mod index;
pub mod query;

use bimap::BiHashMap;
use ordered_float::OrderedFloat;
use roaring::RoaringBitmap;
use std::collections::HashMap;
use std::fmt::{Display, Formatter};

use crate::index::{Indexable, IndexableValue};
use crate::query::{FilterOption, OptionsQueryExecution};
use index::Index;
use query::QueryExecution;
use time::{Date, OffsetDateTime, Time};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FieldValue {
    String(String),
    Numeric(OrderedFloat<f64>),
    Date(OffsetDateTime),
}

impl FieldValue {
    pub fn string(value: String) -> FieldValue {
        FieldValue::String(value)
    }

    pub fn numeric(value: f64) -> FieldValue {
        FieldValue::Numeric(OrderedFloat(value))
    }

    pub fn date(date: Date) -> FieldValue {
        FieldValue::Date(OffsetDateTime::new_utc(date, Time::MIDNIGHT))
    }

    fn as_numeric(&self) -> Option<&OrderedFloat<f64>> {
        if let FieldValue::Numeric(value) = self {
            Some(value)
        } else {
            None
        }
    }

    fn get_numeric(self) -> Option<OrderedFloat<f64>> {
        if let FieldValue::Numeric(value) = self {
            Some(value)
        } else {
            None
        }
    }

    fn as_string(&self) -> Option<&String> {
        if let FieldValue::String(value) = self {
            Some(value)
        } else {
            None
        }
    }

    fn get_string(self) -> Option<String> {
        if let FieldValue::String(value) = self {
            Some(value)
        } else {
            None
        }
    }

    fn get_date_epoch(&self) -> Option<i64> {
        if let FieldValue::Date(value) = self {
            Some(value.unix_timestamp())
        } else {
            None
        }
    }
}

impl Display for FieldValue {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            FieldValue::String(value) => write!(f, "{}", value),
            FieldValue::Numeric(value) => write!(f, "{}", value.0),
            FieldValue::Date(value) => write!(f, "{}", value),
        }
    }
}

pub type DataItemId = usize;

#[derive(Default)]
struct EntityIndices {
    /// Indices available associated by data's field name
    field_indices: HashMap<String, Index>,

    /// Bitmap including all items' positions
    all: RoaringBitmap,
}

impl EntityIndices {
    fn clear(&mut self) {
        self.field_indices.clear();
        self.all.clear();
    }

    fn add_values(&mut self, values: Vec<IndexableValue>, position: u32) {
        for index_value in values {
            // Create index for the key value
            let index = self
                .field_indices
                .entry(index_value.name)
                .or_insert(Index::from_type(&index_value.descriptor));

            index.put(index_value.value, position);
        }
        self.all.insert(position);
    }

    fn remove(&mut self, position: u32) {
        for index in self.field_indices.values_mut() {
            index.remove_item(position);
        }
    }
}

pub struct EntityStorage<T> {
    /// Indices available for the given associated data
    indices: EntityIndices,

    /// Mapping between position of a data item in the index and its ID
    position_id: BiHashMap<u32, DataItemId>,

    /// Data available in the storage associated by the ID
    data: HashMap<DataItemId, T>,
}

impl<T: Indexable> EntityStorage<T> {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn attach<I: IntoIterator<Item = T>>(&mut self, data: I) {
        for item in data {
            self.data.insert(item.id(), item);
        }
    }

    pub fn index(&mut self) {
        self.indices.clear();
        self.position_id.clear();

        for (position, (id, item)) in self.data.iter().enumerate() {
            let position = position as u32;

            self.indices.add_values(item.index_values(), position);

            // Associate index position to the field ID
            self.position_id.insert(position, *id);
        }
    }

    fn add(&mut self, item: T) {
        let id = item.id();

        let position = self
            .position_id
            .get_by_right(&id)
            .copied()
            .unwrap_or(self.position_id.len() as u32);

        self.indices.add_values(item.index_values(), position);

        // Associate index position to the field ID
        self.data.insert(id, item);
        self.position_id.insert(position, id);
    }

    fn remove(&mut self, id: &DataItemId) {
        if let Some((position, _)) = self.position_id.remove_by_right(id) {
            self.data.remove(id);
            self.indices.remove(position);
        }
    }

    fn get_id_by_position(&self, position: &u32) -> Option<&DataItemId> {
        self.position_id.get_by_left(position)
    }

    fn get_position_by_id(&self, id: &DataItemId) -> Option<&u32> {
        self.position_id.get_by_right(id)
    }
}

impl<T> Default for EntityStorage<T> {
    fn default() -> Self {
        EntityStorage {
            indices: Default::default(),
            position_id: Default::default(),
            data: Default::default(),
        }
    }
}

pub struct Engine<T> {
    storage: EntityStorage<T>,
}

impl<T> Engine<T> {
    pub fn new(storage: EntityStorage<T>) -> Self {
        Engine { storage }
    }
}

impl<T: Indexable + Clone> Engine<T> {
    pub fn query(&self, execution: QueryExecution<T>) -> Vec<T> {
        execution.run(&self.storage)
    }

    pub fn options(&self, execution: OptionsQueryExecution<T>) -> Vec<FilterOption> {
        execution.run(&self.storage)
    }

    pub fn add(&mut self, item: T) {
        self.storage.add(item);
    }

    pub fn remove(&mut self, id: &DataItemId) {
        self.storage.remove(id);
    }
}

#[cfg(test)]
mod tests {
    use crate::fixtures::{
        create_player_from_index, create_players_storage, create_random_players,
        DecreaseScoreDelta, Player, Sport, SwitchSportsDelta,
    };
    use crate::query::{
        CompositeFilter, FilterOption, OptionsQueryExecution, Pagination, QueryExecution, Sort,
        SortDirection,
    };
    use crate::{Engine, FieldValue};
    use lazy_static::lazy_static;
    use std::collections::HashMap;
    use time::{Date, Month};

    lazy_static! {
        static ref MICHAEL_JORDAN: Player =
            Player::new(0, "Michael Jordan", Sport::Basketball, "1963-02-17").with_score(10.0);
        static ref LIONEL_MESSI: Player =
            Player::new(1, "Lionel Messi", Sport::Football, "1987-06-24").with_score(9.0);
        static ref CRISTIANO_RONALDO: Player =
            Player::new(2, "Cristiano Ronaldo", Sport::Football, "1985-02-05").with_score(9.0);
        static ref ROGER: Player =
            Player::new(3, "Roger", Sport::Football, "1996-05-01").with_score(5.0);
        static ref DAVID: Player = Player::new(4, "David", Sport::Football, "1974-10-01");
    }

    #[test]
    fn query_enum_eq_filter() {
        // given
        let storage = create_players_storage(vec![
            MICHAEL_JORDAN.clone(),
            LIONEL_MESSI.clone(),
            CRISTIANO_RONALDO.clone(),
        ]);
        let engine = Engine::new(storage);

        let filter = CompositeFilter::eq("sport", FieldValue::string("football".to_string()));

        // when
        let mut matches = engine.query(QueryExecution::new().with_filter(filter));

        // then
        matches.sort_by(|a, b| a.id.cmp(&b.id));

        assert_eq!(
            matches,
            vec![LIONEL_MESSI.clone(), CRISTIANO_RONALDO.clone()]
        );
    }

    #[test]
    fn query_date_ge_filter() {
        // given
        let storage = create_players_storage(vec![
            MICHAEL_JORDAN.clone(),
            LIONEL_MESSI.clone(),
            CRISTIANO_RONALDO.clone(),
            ROGER.clone(),
        ]);
        let engine = Engine::new(storage);

        let filter = CompositeFilter::ge(
            "birth_date",
            FieldValue::date(Date::from_calendar_date(1990, Month::January, 1).unwrap()),
        );

        // when
        let mut matches = engine.query(QueryExecution::new().with_filter(filter));

        // then
        matches.sort_by(|a, b| a.id.cmp(&b.id));

        assert_eq!(matches, vec![ROGER.clone()]);
    }

    #[test]
    fn query_date_between_filter() {
        // given
        let storage = create_players_storage(vec![
            MICHAEL_JORDAN.clone(),
            LIONEL_MESSI.clone(),
            CRISTIANO_RONALDO.clone(),
            ROGER.clone(),
        ]);
        let engine = Engine::new(storage);

        let filter = CompositeFilter::between(
            "birth_date",
            FieldValue::date(Date::from_calendar_date(1970, Month::January, 1).unwrap()),
            FieldValue::date(Date::from_calendar_date(1990, Month::January, 1).unwrap()),
        );

        // when
        let mut matches = engine.query(QueryExecution::new().with_filter(filter));

        // then
        matches.sort_by(|a, b| a.id.cmp(&b.id));

        assert_eq!(
            matches,
            vec![LIONEL_MESSI.clone(), CRISTIANO_RONALDO.clone()]
        );
    }

    #[test]
    fn query_numeric_between_filter() {
        // given
        let storage = create_players_storage(vec![
            MICHAEL_JORDAN.clone(),
            LIONEL_MESSI.clone(),
            ROGER.clone(),
        ]);
        let engine = Engine::new(storage);

        let filter =
            CompositeFilter::between("score", FieldValue::numeric(6.0), FieldValue::numeric(10.0));

        // when
        let mut matches = engine.query(QueryExecution::new().with_filter(filter));

        // then
        matches.sort_by(|a, b| a.id.cmp(&b.id));

        assert_eq!(matches, vec![MICHAEL_JORDAN.clone(), LIONEL_MESSI.clone()]);
    }

    #[test]
    fn query_numeric_ge_filter() {
        // given
        let storage = create_players_storage(vec![
            MICHAEL_JORDAN.clone(),
            LIONEL_MESSI.clone(),
            ROGER.clone(),
        ]);
        let engine = Engine::new(storage);

        let filter = CompositeFilter::ge("score", FieldValue::numeric(6.0));

        // when
        let mut matches = engine.query(QueryExecution::new().with_filter(filter));

        // then
        matches.sort_by(|a, b| a.id.cmp(&b.id));

        assert_eq!(matches, vec![MICHAEL_JORDAN.clone(), LIONEL_MESSI.clone()]);
    }

    #[test]
    fn query_numeric_le_filter() {
        // given
        let storage = create_players_storage(vec![
            MICHAEL_JORDAN.clone(),
            LIONEL_MESSI.clone(),
            ROGER.clone(),
        ]);
        let engine = Engine::new(storage);

        let filter = CompositeFilter::le("score", FieldValue::numeric(6.0));

        // when
        let mut matches = engine.query(QueryExecution::new().with_filter(filter));

        // then
        matches.sort_by(|a, b| a.id.cmp(&b.id));

        assert_eq!(matches, vec![ROGER.clone()]);
    }

    #[test]
    fn query_not_filter() {
        // given
        let storage = create_players_storage(vec![
            MICHAEL_JORDAN.clone(),
            LIONEL_MESSI.clone(),
            CRISTIANO_RONALDO.clone(),
            ROGER.clone(),
        ]);
        let engine = Engine::new(storage);

        let filter = CompositeFilter::negate(CompositeFilter::eq(
            "sport",
            FieldValue::String(Sport::Basketball.as_string()),
        ));

        // when
        let mut matches = engine.query(QueryExecution::new().with_filter(filter));

        // then
        matches.sort_by(|a, b| a.id.cmp(&b.id));

        assert_eq!(
            matches,
            vec![
                LIONEL_MESSI.clone(),
                CRISTIANO_RONALDO.clone(),
                ROGER.clone()
            ]
        );
    }

    #[test]
    fn query_numeric_delta() {
        // given
        let storage = create_players_storage(vec![
            MICHAEL_JORDAN.clone(),
            LIONEL_MESSI.clone(),
            CRISTIANO_RONALDO.clone(),
        ]);
        let engine = Engine::new(storage);

        let deltas = vec![
            DecreaseScoreDelta::new(MICHAEL_JORDAN.id, MICHAEL_JORDAN.score.unwrap()),
            DecreaseScoreDelta::new(LIONEL_MESSI.id, LIONEL_MESSI.score.unwrap()),
        ];
        let filter = CompositeFilter::eq("sport", FieldValue::string("football".to_string()));

        // when
        let mut matches = engine.query(
            QueryExecution::new()
                .with_filter(filter)
                .with_deltas(deltas),
        );

        // then
        matches.sort_by(|a, b| a.id.cmp(&b.id));

        assert_eq!(
            matches,
            vec![
                Player {
                    id: LIONEL_MESSI.id,
                    name: LIONEL_MESSI.name.to_string(),
                    score: Some(8.0),
                    sport: LIONEL_MESSI.sport.clone(),
                    birth_date: LIONEL_MESSI.birth_date.clone()
                },
                CRISTIANO_RONALDO.clone()
            ]
        );
    }

    #[test]
    fn query_enum_delta() {
        // given
        let storage = create_players_storage(vec![
            MICHAEL_JORDAN.clone(),
            LIONEL_MESSI.clone(),
            CRISTIANO_RONALDO.clone(),
        ]);
        let engine = Engine::new(storage);

        let deltas = vec![SwitchSportsDelta::new(
            MICHAEL_JORDAN.id,
            MICHAEL_JORDAN.sport.clone(),
            Sport::Football,
        )];
        let filter = CompositeFilter::eq("sport", FieldValue::string("football".to_string()));

        // when
        let mut matches = engine.query(
            QueryExecution::new()
                .with_filter(filter)
                .with_deltas(deltas),
        );

        // then
        matches.sort_by(|a, b| a.id.cmp(&b.id));

        assert_eq!(
            matches,
            vec![
                Player {
                    id: MICHAEL_JORDAN.id,
                    name: MICHAEL_JORDAN.name.to_string(),
                    score: MICHAEL_JORDAN.score,
                    sport: Sport::Football,
                    birth_date: MICHAEL_JORDAN.birth_date.clone()
                },
                LIONEL_MESSI.clone(),
                CRISTIANO_RONALDO.clone()
            ]
        );
    }

    #[test]
    fn query_pagination() {
        // given
        let storage = create_players_storage(create_random_players(20));
        let engine = Engine::new(storage);

        let filter = CompositeFilter::eq("sport", FieldValue::string("football".to_string()));
        let sort = Sort::new("score");
        let pagination = Pagination::new(2, 5);

        // when
        let matches = engine.query(
            QueryExecution::new()
                .with_filter(filter)
                .with_sort(sort)
                .with_pagination(pagination),
        );

        // then
        assert_eq!(
            matches,
            vec![
                create_player_from_index(5),
                create_player_from_index(7),
                create_player_from_index(9),
                create_player_from_index(11),
                create_player_from_index(13)
            ]
        );
    }

    #[test]
    fn query_sort_numeric_asc() {
        // given
        let storage = create_players_storage(vec![
            MICHAEL_JORDAN.clone(),
            CRISTIANO_RONALDO.clone(),
            ROGER.clone(),
            DAVID.clone(),
        ]);
        let engine = Engine::new(storage);

        let sort = Sort::new("score").with_direction(SortDirection::ASC);

        // when
        let matches = engine.query(QueryExecution::new().with_sort(sort));

        // then
        assert_eq!(
            matches,
            vec![
                ROGER.clone(),
                CRISTIANO_RONALDO.clone(),
                MICHAEL_JORDAN.clone(),
                DAVID.clone()
            ]
        );
    }

    #[test]
    fn query_sort_numeric_desc() {
        // given
        let storage = create_players_storage(vec![
            MICHAEL_JORDAN.clone(),
            CRISTIANO_RONALDO.clone(),
            ROGER.clone(),
            DAVID.clone(),
        ]);
        let engine = Engine::new(storage);

        let sort = Sort::new("score").with_direction(SortDirection::DESC);

        // when
        let matches = engine.query(QueryExecution::new().with_sort(sort));

        // then
        assert_eq!(
            matches,
            vec![
                MICHAEL_JORDAN.clone(),
                CRISTIANO_RONALDO.clone(),
                ROGER.clone(),
                DAVID.clone()
            ]
        );
    }

    #[test]
    fn compute_all_filter_options() {
        // given
        let storage = create_players_storage(vec![
            MICHAEL_JORDAN.clone(),
            CRISTIANO_RONALDO.clone(),
            LIONEL_MESSI.clone(),
            ROGER.clone(),
            DAVID.clone(),
        ]);
        let engine = Engine::new(storage);

        // when
        let mut filter_options = engine.options(OptionsQueryExecution::new());

        // then
        filter_options.sort_by(|a, b| a.field.cmp(&b.field));

        assert_eq!(
            filter_options,
            vec![
                FilterOption::new("birth_date".to_string(), HashMap::from_iter([])),
                FilterOption::new(
                    "name".to_string(),
                    HashMap::from_iter([
                        ("Cristiano Ronaldo".to_string(), 1),
                        ("Michael Jordan".to_string(), 1),
                        ("Lionel Messi".to_string(), 1),
                        ("Roger".to_string(), 1),
                        ("David".to_string(), 1)
                    ])
                ),
                FilterOption::new(
                    "score".to_string(),
                    HashMap::from_iter([
                        ("5".to_string(), 1),
                        ("9".to_string(), 2),
                        ("10".to_string(), 1)
                    ])
                ),
                FilterOption::new(
                    "sport".to_string(),
                    HashMap::from_iter([
                        ("basketball".to_string(), 1),
                        ("football".to_string(), 4)
                    ])
                )
            ]
        );
    }

    #[test]
    fn compute_all_filter_options_with_filter() {
        // given
        let storage = create_players_storage(vec![
            MICHAEL_JORDAN.clone(),
            CRISTIANO_RONALDO.clone(),
            LIONEL_MESSI.clone(),
            ROGER.clone(),
            DAVID.clone(),
        ]);
        let engine = Engine::new(storage);
        let filter = CompositeFilter::ge("score", FieldValue::numeric(8.0));

        // when
        let mut filter_options = engine.options(OptionsQueryExecution::new().with_filter(filter));

        // then
        filter_options.sort_by(|a, b| a.field.cmp(&b.field));

        assert_eq!(
            filter_options,
            vec![
                FilterOption::new("birth_date".to_string(), HashMap::from_iter([])),
                FilterOption::new(
                    "name".to_string(),
                    HashMap::from_iter([
                        ("Cristiano Ronaldo".to_string(), 1),
                        ("Michael Jordan".to_string(), 1),
                        ("Lionel Messi".to_string(), 1)
                    ])
                ),
                FilterOption::new(
                    "score".to_string(),
                    HashMap::from_iter([("9".to_string(), 2), ("10".to_string(), 1)])
                ),
                FilterOption::new(
                    "sport".to_string(),
                    HashMap::from_iter([
                        ("basketball".to_string(), 1),
                        ("football".to_string(), 2)
                    ])
                )
            ]
        );
    }

    #[test]
    fn add_item() {
        // given
        let storage = create_players_storage(vec![
            MICHAEL_JORDAN.clone(),
            LIONEL_MESSI.clone(),
            CRISTIANO_RONALDO.clone(),
        ]);
        let mut engine = Engine::new(storage);

        // when
        engine.add(ROGER.clone());

        // then
        let query = QueryExecution::new().with_filter(CompositeFilter::eq(
            "name",
            FieldValue::String(ROGER.name.to_string()),
        ));
        let matches = engine.query(query);

        assert_eq!(matches, vec![ROGER.clone()]);
    }

    #[test]
    fn remove_item() {
        // given
        let storage = create_players_storage(vec![
            MICHAEL_JORDAN.clone(),
            LIONEL_MESSI.clone(),
            CRISTIANO_RONALDO.clone(),
        ]);
        let mut engine = Engine::new(storage);

        // when
        engine.remove(&CRISTIANO_RONALDO.id);

        // then
        let query = QueryExecution::new().with_filter(CompositeFilter::eq(
            "name",
            FieldValue::String(CRISTIANO_RONALDO.name.to_string()),
        ));
        let matches = engine.query(query);

        assert!(matches.is_empty());
    }
}
