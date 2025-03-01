use crate::models::TileStorage;
use diesel::ExpressionMethods;
use diesel::QueryDsl;
use diesel::SelectableHelper;
use diesel_async::AsyncPgConnection;
use diesel_async::RunQueryDsl;
use diesel_async::pooled_connection::AsyncDieselConnectionManager;

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

pub async fn delete_in_bbox(
    conn: &mut bb8::PooledConnection<'_, AsyncDieselConnectionManager<AsyncPgConnection>>,
    category: TileCategory,
    z: u8,
    min_x: i32,
    max_x: i32,
    min_y: i32,
    max_y: i32,
) -> Result<(), anyhow::Error> {
    let category_i16 = tile_enum_to_i16(category);

    let _ = diesel::delete(
        crate::schema::gtfs::tile_storage::dsl::tile_storage
            .filter(crate::schema::gtfs::tile_storage::dsl::category.eq(category_i16))
            .filter(crate::schema::gtfs::tile_storage::dsl::x.ge(min_x))
            .filter(crate::schema::gtfs::tile_storage::dsl::x.le(max_x))
            .filter(crate::schema::gtfs::tile_storage::dsl::y.ge(min_y))
            .filter(crate::schema::gtfs::tile_storage::dsl::y.le(max_y)),
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

pub fn delete_all_tiles_in_bbox(
    conn: &mut bb8::PooledConnection<'_, AsyncDieselConnectionManager<AsyncPgConnection>>,
    rect: &geo::Rect<f64>,
    z: u8,
    c: TileCategory,
) -> Result<(), anyhow::Error> {
    //make bbox
    let bbox = slippy_map_tiles::BBox::new(
        rect.max().y as f32,
        rect.min().x as f32,
        rect.min().y as f32,
        rect.max().x as f32,
    );

    Ok(())
}
