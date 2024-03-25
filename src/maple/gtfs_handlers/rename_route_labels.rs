pub fn rename_route_string(x: String) -> String {
    x.replace("Counterclockwise", "ACW")
        .replace("counterclockwise", "ACW")
        .replace("clockwise", "CW")
        .replace("Clockwise", "CW")
}
