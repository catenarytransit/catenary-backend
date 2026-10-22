use prost_build::Config;

fn main() {
    #[cfg(target_os = "linux")]
    if let Ok(dir) = std::env::var("DEP_CATENARY_OSR_BRIDGE_LIB_DIR") {
        println!("cargo:rustc-link-arg=-Wl,-rpath,{dir}");
        println!("cargo:rustc-link-arg=-Wl,-rpath,$ORIGIN/lib");
    }

    Config::new()
        .type_attribute(
            ".",
            "#[derive(Deserialize,Serialize)] #[serde(rename_all = \"snake_case\")]",
        )
        .compile_protos(&["src/gtfs_schedule_proto.proto"], &["src/"])
        .unwrap();

    Config::new()
        .type_attribute(
            ".",
            "#[derive(Deserialize,Serialize)] #[serde(rename_all = \"snake_case\")]",
        )
        .compile_protos(
            &["src/gtfs_realtime_extensions/gtfs-realtime-MTARR.proto"],
            &["src/"],
        )
        .unwrap();

    Config::new()
        .type_attribute(
            ".",
            "#[derive(Deserialize,Serialize)] #[serde(rename_all = \"snake_case\")]",
        )
        .compile_protos(
            &["src/gtfs_realtime_extensions/gtfs-realtime-NYCT.proto"],
            &["src/"],
        )
        .unwrap();
}
