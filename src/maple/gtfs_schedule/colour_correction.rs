use rgb::RGB;

pub const WHITE_RGB: RGB<u8> = RGB::new(255, 255, 255);

pub fn fix_background_colour(input: &str) -> &str {
    if input == "ffffff" || input == "000000" {
        "0ea5e9"
    } else {
        input
    }
}

pub fn fix_background_colour_rgb(background: RGB<u8>) -> RGB<u8> {
    if background == RGB::new(255, 255, 255) || background == RGB::new(0, 0, 0) {
        RGB::new(14, 165, 233)
    } else {
        background.clone()
    }
}

pub fn fix_background_colour_rgb_feed(feed_id: &String, background: RGB<u8>) -> RGB<u8> {
    match feed_id.as_str() {
        "f-9q5b-longbeachtransit" => {
            if (background == WHITE_RGB) {
                return RGB::new(128, 31, 58);
            } else {
                return fix_background_colour_rgb(background);
            }
        }
        "f-9-amtrak~amtrakcalifornia~amtrakcharteredvehicle" => RGB::new(23, 114, 172),
        "f-9q5-metro~losangeles" => {
            if (background == WHITE_RGB) {
                return RGB::new(225, 103, 16);
            } else {
                return fix_background_colour_rgb(background);
            }
        }
        _ => fix_background_colour_rgb(background),
    }
}

pub fn fix_background_colour_rgb_feed_route(
    feed_id: &String,
    background: RGB<u8>,
    route: &gtfs_structures::Route,
) -> RGB<u8> {
    match feed_id.as_str() {
        "f-9q5b-longbeachtransit" => {
            if background == WHITE_RGB {
                return RGB::new(128, 31, 58);
            } else {
                return fix_background_colour_rgb(background);
            }
        }
        "f-9q5-metro~losangeles" => {
            if background == WHITE_RGB {
                return RGB::new(225, 103, 16);
            } else {
                let metroid = &route.id.split("-").collect::<Vec<&str>>()[0];

                if metroid.len() == 3 && metroid.chars().nth(0).unwrap() == '7' {
                    return RGB::new(209, 18, 66);
                }
                return fix_background_colour_rgb(background);
            }
        }
        "f-9-amtrak~amtrakcalifornia~amtrakcharteredvehicle" => RGB::new(23, 114, 172),
        "f-9mu-mts" => match route.id.as_str() {
            "280" => RGB::new(7, 103, 56),
            "290" => RGB::new(235, 194, 22),
            "237" => RGB::new(96, 41, 133),
            "201" => RGB::new(232, 93, 152),
            "202" => RGB::new(232, 93, 152),
            "204" => RGB::new(232, 93, 152),
            "235" => RGB::new(238, 45, 36),
            "215" => RGB::new(59, 192, 225),
            "225" => RGB::new(41, 52, 144),
            "227" => RGB::new(123, 194, 77),
            _ => fix_background_colour_rgb(background),
        },
        _ => fix_background_colour_rgb(background),
    }
}

pub fn fix_foreground_colour<'a>(background: &'a str, foreground: &'a str) -> &'a str {
    if background == foreground {
        "000000"
    } else {
        foreground
    }
}

pub fn fix_foreground_colour_rgb(background: RGB<u8>, foreground: RGB<u8>) -> RGB<u8> {
    if background == foreground {
        RGB::new(0, 0, 0)
    } else {
        foreground.clone()
    }
}

pub fn fix_foreground_colour_rgb_feed(
    feed_id: &String,
    background: RGB<u8>,
    foreground: RGB<u8>,
) -> RGB<u8> {
    match feed_id.as_str() {
        "f-9q5b-longbeachtransit" => WHITE_RGB,
        _ => fix_foreground_colour_rgb(background, foreground),
    }
}
