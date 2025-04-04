//cache the intercity rail feed from level 4-9

use diesel::prelude::*;
use diesel_async::RunQueryDsl;
use slippy_map_tiles::BBox;

#[derive(Debug, Clone, Copy)]
pub enum Category {
    IntercityRailOriginal,
    LocalRailOriginal,
    BusOriginal,
    FerryOriginal,
}

fn tile_category_to_number(category: &Category) -> i16 {
    match category {
        Category::IntercityRailOriginal => 1,
        Category::LocalRailOriginal => 2,
        Category::BusOriginal => 3,
        Category::FerryOriginal => 4,
    }
}

pub async fn delete_tile_storage_for_bbox_and_category_zoom(
    conn: &mut bb8::PooledConnection<
        '_,
        diesel_async::pooled_connection::AsyncDieselConnectionManager<
            diesel_async::AsyncPgConnection,
        >,
    >,
    bbox: &BBox,
    category: &Category,
    zoom: u8,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let tile_category = tile_category_to_number(category);

    let nw_corner = bbox.nw_corner();
    let se_corner = bbox.se_corner();

    //get bounding tile coords

    let nw_corner_tile = nw_corner.tile(zoom);
    let se_corner_tile = se_corner.tile(zoom);

    let left_x = nw_corner_tile.x();
    let right_x = se_corner_tile.x();
    let top_y = nw_corner_tile.y();
    let bottom_y = se_corner_tile.y();

    /*
    for tile in bbox.tiles_for_zoom(zoom) {
        let x = tile.x();
        let y = tile.y();

        let delete_from_pg = diesel::delete(
            crate::schema::gtfs::tile_storage::dsl::tile_storage.filter(
                crate::schema::gtfs::tile_storage::dsl::x
                    .eq(x as i32)
                    .and(crate::schema::gtfs::tile_storage::dsl::y.eq(y as i32))
                    .and(crate::schema::gtfs::tile_storage::dsl::z.eq(zoom as i16))
                    .and(crate::schema::gtfs::tile_storage::dsl::category.eq(&tile_category)),
            ),
        )
        .execute(conn)
        .await?;
    }*/

    let delete_from_pg = diesel::delete(
        crate::schema::gtfs::tile_storage::dsl::tile_storage.filter(
            crate::schema::gtfs::tile_storage::dsl::x
                .ge(left_x as i32)
                .and(crate::schema::gtfs::tile_storage::dsl::x.le(right_x as i32))
                .and(crate::schema::gtfs::tile_storage::dsl::y.le(bottom_y as i32))
                .and(crate::schema::gtfs::tile_storage::dsl::y.ge(top_y as i32))
                .and(crate::schema::gtfs::tile_storage::dsl::z.eq(zoom as i16))
                .and(crate::schema::gtfs::tile_storage::dsl::category.eq(&tile_category)),
        ),
    )
    .execute(conn)
    .await?;

    Ok(())
}

pub async fn wipe_all_relevant_tiles_for_bbox(
    conn: &mut bb8::PooledConnection<
        '_,
        diesel_async::pooled_connection::AsyncDieselConnectionManager<
            diesel_async::AsyncPgConnection,
        >,
    >,
    bbox: &BBox,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    for z in 4u8..=11u8 {
        let _ = delete_tile_storage_for_bbox_and_category_zoom(
            conn,
            bbox,
            &Category::IntercityRailOriginal,
            z,
        )
        .await?;

        let _ =
            delete_tile_storage_for_bbox_and_category_zoom(conn, bbox, &Category::BusOriginal, z)
                .await?;
    }

    Ok(())
}

pub async fn fetch_tile(
    conn: &mut bb8::PooledConnection<
        '_,
        diesel_async::pooled_connection::AsyncDieselConnectionManager<
            diesel_async::AsyncPgConnection,
        >,
    >,
    x: u32,
    y: u32,
    z: u8,
    category: Category,
) -> Result<Vec<crate::models::TileStorage>, Box<dyn std::error::Error + Send + Sync>> {
    let tile_category = tile_category_to_number(&category);

    let tile = crate::schema::gtfs::tile_storage::dsl::tile_storage
        .filter(crate::schema::gtfs::tile_storage::dsl::x.eq(x as i32))
        .filter(crate::schema::gtfs::tile_storage::dsl::y.eq(y as i32))
        .filter(crate::schema::gtfs::tile_storage::dsl::z.eq(z as i16))
        .filter(crate::schema::gtfs::tile_storage::dsl::category.eq(tile_category))
        .load::<crate::models::TileStorage>(conn)
        .await?;

    Ok(tile)
}

pub async fn insert_tile(
    conn: &mut bb8::PooledConnection<
        '_,
        diesel_async::pooled_connection::AsyncDieselConnectionManager<
            diesel_async::AsyncPgConnection,
        >,
    >,
    x: u32,
    y: u32,
    z: u8,
    category: Category,
    mvt_data: Vec<u8>,
    added_time: chrono::DateTime<chrono::Utc>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let tile_category = tile_category_to_number(&category);

    let tile = crate::models::TileStorage {
        x: x as i32,
        y: y as i32,
        z: z as i16,
        category: tile_category,
        mvt_data,
        added_time,
    };

    diesel::insert_into(crate::schema::gtfs::tile_storage::dsl::tile_storage)
        .values(tile)
        .execute(conn)
        .await?;

    Ok(())
}
