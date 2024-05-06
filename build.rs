use prost_build::Config;

fn main() {
    Config::new()
        .type_attribute(
            ".",
            "#[derive(Deserialize,Serialize)] #[serde(rename_all = \"snake_case\")]",
        )
        .compile_protos(&["src/gtfs_schedule_proto.proto"], &["src/"])
        .unwrap();
}
