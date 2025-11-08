#[actix_web::get("/getroutesofchateau/{chateau}")]
async fn routesofchateau(
    pool: web::Data<Arc<CatenaryPostgresPool>>,
    path: web::Path<String>,
    req: HttpRequest,
) -> impl Responder {
    let conn_pool = pool.as_ref();
    let conn_pre = conn_pool.get().await;
    let conn = &mut conn_pre.unwrap();

    let chateau_id = path.into_inner();

    use catenary::schema::gtfs::routes as routes_pg_schema;

    let routes = routes_pg_schema::dsl::routes
        .filter(routes_pg_schema::dsl::chateau.eq(&chateau_id))
        .select(catenary::models::Route::as_select())
        .load::<catenary::models::Route>(conn)
        .await
        .unwrap();

    HttpResponse::Ok()
        .insert_header(("Content-Type", "application/json"))
        .insert_header(("Cache-Control", "max-age=3600"))
        .body(serde_json::to_string(&routes).unwrap())
}