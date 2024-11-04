use crate::models::TileStorage;
use diesel::ExpressionMethods;
use diesel::QueryDsl;
use diesel::SelectableHelper;
use diesel_async::pooled_connection::AsyncDieselConnectionManager;
use diesel_async::AsyncPgConnection;
use diesel_async::RunQueryDsl;

pub enum TileCategory {
    IntercityRailShapesRaw,
    LocalRailShapesRaw,
    BusShapesRaw,
}

fn tile_enum_to_i16(x: TileCategory) -> i16 {
    match x {
        TileCategory::IntercityRailShapesRaw => 0,
        TileCategory::LocalRailShapesRaw => 1,
        TileCategory::BusShapesRaw => 2,
    }
}

pub async fn insert_tile(
    conn: &mut bb8::PooledConnection<'_, AsyncDieselConnectionManager<AsyncPgConnection>>,
    category: TileCategory,
    z: i16,
    x: i32,
    y: i32,
    data: Vec<u8>,
) -> Result<(), anyhow::Error> {
    let category_i16 = tile_enum_to_i16(category);

    let _ = diesel::insert_into(crate::schema::gtfs::tile_storage::dsl::tile_storage)
        .values(crate::models::TileStorage {
            category: category_i16,
            z: z,
            x: x,
            y: y,
            mvt_data: data,
            added_time: chrono::Utc::now(),
        })
        .execute(conn)
        .await?;

    Ok(())
}

pub async fn delete_tile(
    conn: &mut bb8::PooledConnection<'_, AsyncDieselConnectionManager<AsyncPgConnection>>,
    category: TileCategory,
    z: i16,
    x: i32,
    y: i32,
) -> Result<(), anyhow::Error> {
    let category_i16 = tile_enum_to_i16(category);

    let _ = diesel::delete(
        crate::schema::gtfs::tile_storage::dsl::tile_storage
            .filter(crate::schema::gtfs::tile_storage::dsl::category.eq(category_i16))
            .filter(crate::schema::gtfs::tile_storage::dsl::z.eq(z))
            .filter(crate::schema::gtfs::tile_storage::dsl::x.eq(x))
            .filter(crate::schema::gtfs::tile_storage::dsl::y.eq(y)),
    );

    Ok(())
}

pub async fn get_tile(
    conn: &mut bb8::PooledConnection<'_, AsyncDieselConnectionManager<AsyncPgConnection>>,
    category: TileCategory,
    z: i16,
    x: i32,
    y: i32,
) -> diesel::result::QueryResult<crate::models::TileStorage> {
    let category_i16 = tile_enum_to_i16(category);

    crate::schema::gtfs::tile_storage::dsl::tile_storage
        .filter(crate::schema::gtfs::tile_storage::dsl::category.eq(category_i16))
        .filter(crate::schema::gtfs::tile_storage::dsl::z.eq(z))
        .filter(crate::schema::gtfs::tile_storage::dsl::x.eq(x))
        .filter(crate::schema::gtfs::tile_storage::dsl::y.eq(y))
        .select(crate::models::TileStorage::as_select())
        .first::<crate::models::TileStorage>(conn)
        .await
}