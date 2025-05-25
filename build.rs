fn main() {
    lalrpop::Configuration::new()
        .use_cargo_dir_conventions()
        .set_in_dir("lib/src")
        .process()
        .unwrap();
}
