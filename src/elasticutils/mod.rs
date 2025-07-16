use elasticsearch::{
    Elasticsearch,
    auth::Credentials,
    cat::CatIndicesParts,
    http::{
        Url,
        transport::{SingleNodeConnectionPool, TransportBuilder},
    },
    indices::{IndicesCreateParts, IndicesPutMappingParts},
};
use serde_json::{Value, json};
use std::error::Error;

pub fn single_elastic_connect(server_url: &str) -> Result<Elasticsearch, Box<dyn Error + Sync + Send>> {
    let url = Url::parse(server_url)?;
    let conn_pool = SingleNodeConnectionPool::new(url);
    let transport = TransportBuilder::new(conn_pool).disable_proxy().build()?;
    let client = Elasticsearch::new(transport);

    Ok(client)
}

pub fn make_index_and_mappings() -> Result<(), Box<dyn Error + Sync + Send>> {
    unimplemented!();
    Ok(())
}
