fn main() {
    #[cfg(target_os = "linux")]
    {
        if let Ok(dir) = std::env::var("DEP_CATENARY_OSR_BRIDGE_LIB_DIR") {
            println!("cargo:rustc-link-arg=-Wl,-rpath,{dir}");
        }
        println!("cargo:rustc-link-arg=-Wl,-rpath,$ORIGIN/lib");
    }
}
