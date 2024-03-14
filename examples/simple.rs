use lazy_static::lazy_static;
use time::{Date, Month};

use delta_db::{Engine, FieldValue};
use delta_db::fixtures::{Player, Sport, SwitchSportsDelta};
use delta_db::query::{
    CompositeFilter, OptionsQueryExecution, QueryExecution, Sort, SortDirection,
};
use delta_db::storage::StorageBuilder;

lazy_static! {
    static ref MICHAEL_JORDAN: Player =
        Player::new(0, "Michael Jordan", Sport::Basketball, "1963-02-17").with_score(10.0);
    static ref LIONEL_MESSI: Player =
        Player::new(1, "Lionel Messi", Sport::Football, "1987-06-24").with_score(9.0);
    static ref CRISTIANO_RONALDO: Player =
        Player::new(2, "Cristiano Ronaldo", Sport::Football, "1985-02-05").with_score(9.0);
    static ref ROGER: Player =
        Player::new(3, "Roger", Sport::Basketball, "1996-05-01").with_score(5.0);
    static ref DAVID: Player = Player::new(4, "David", Sport::Basketball, "1974-10-01");
}

fn main() {
    println!("Welcome to the simple Player search!");

    let mut storage = StorageBuilder::disk().build();

    storage.carry(vec![
        MICHAEL_JORDAN.clone(),
        LIONEL_MESSI.clone(),
        CRISTIANO_RONALDO.clone(),
        ROGER.clone(),
        DAVID.clone(),
    ]);

    let mut engine = Engine::new(storage);

    let filter_options = engine.options(OptionsQueryExecution::new());

    println!("Filter possibilities:\n{:?}\n", filter_options);

    let query = QueryExecution::new()
        .with_filter(CompositeFilter::eq(
            "sport",
            FieldValue::string(Sport::Basketball.as_string()),
        ))
        .with_sort(Sort::new("score").with_direction(SortDirection::DESC));
    let players = engine.query(query);

    let player_names: Vec<&str> = players.iter().map(|player| player.name.as_str()).collect();
    println!(
        "Basketball players sorted by score: {}",
        player_names.join(", ")
    );

    let players = engine.query(
        QueryExecution::new()
            .with_filter(CompositeFilter::between(
                "birth_date",
                FieldValue::date(Date::from_calendar_date(1980, Month::January, 1).unwrap()),
                FieldValue::date(Date::from_calendar_date(1989, Month::December, 31).unwrap()),
            ))
            .with_sort(Sort::new("name").with_direction(SortDirection::ASC)),
    );

    let player_names: Vec<&str> = players.iter().map(|player| player.name.as_str()).collect();
    println!("Players born in the 80s: {}", player_names.join(", "));

    let switch_sports = vec![
        SwitchSportsDelta::new(
            MICHAEL_JORDAN.id,
            MICHAEL_JORDAN.sport.clone(),
            Sport::Football,
        ),
        SwitchSportsDelta::new(
            CRISTIANO_RONALDO.id,
            CRISTIANO_RONALDO.sport.clone(),
            Sport::Basketball,
        ),
    ];

    let query = QueryExecution::new()
        .with_filter(CompositeFilter::eq(
            "sport",
            FieldValue::string(Sport::Basketball.as_string()),
        ))
        .with_sort(Sort::new("score").with_direction(SortDirection::DESC))
        .with_deltas(switch_sports);
    let players = engine.query(query);

    let player_names: Vec<&str> = players.iter().map(|player| player.name.as_str()).collect();
    println!(
        "Basketball players sorted by score after switching sports: {}",
        player_names.join(", ")
    );

    engine.remove(&DAVID.id);

    println!(
        "Basketball players sorted by score: {}",
        player_names.join(", ")
    );

    let players = engine.query(
        QueryExecution::new()
            .with_filter(CompositeFilter::eq(
                "sport",
                FieldValue::string(Sport::Basketball.as_string()),
            ))
    );

    let player_names: Vec<&str> = players.iter().map(|player| player.name.as_str()).collect();
    println!("Players playing basketball after deletion: {}", player_names.join(", "));
}
