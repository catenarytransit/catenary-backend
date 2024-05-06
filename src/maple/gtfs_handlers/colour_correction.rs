use rgb::RGB;

pub const WHITE_RGB: RGB<u8> = RGB::new(255, 255, 255);

pub fn fix_background_colour_rgb(background: RGB<u8>) -> RGB<u8> {
    if background == RGB::new(255, 255, 255) || background == RGB::new(0, 0, 0) {
        RGB::new(14, 165, 233)
    } else {
        background.clone()
    }
}

pub fn fix_background_colour_rgb_feed_route(
    feed_id: &str,
    background: RGB<u8>,
    route: &gtfs_structures::Route,
) -> RGB<u8> {
    match feed_id {
        "f-9q5b-longbeachtransit" => {
            match route.id.as_str() {
                "1" => RGB::new(247, 161, 129),
                "2" => RGB::new(228, 228, 23),
                "8" => RGB::new(0, 167, 78),
                "22" => RGB::new(167, 66, 62),
                //passport
                "37" => RGB::new(154, 75, 157),
                "41" => RGB::new(238, 41, 46),
                "45" => RGB::new(0, 134, 172),
                "46" => RGB::new(116, 129, 55),
                "51" => RGB::new(170, 96, 161),
                "61" => RGB::new(238, 41, 46),
                "71" => RGB::new(4, 82, 161),
                "91" => RGB::new(240, 94, 140),
                "92" => RGB::new(240, 126, 72),
                "93" => RGB::new(173, 113, 175),
                "94" => RGB::new(240, 94, 140),
                "101" => RGB::new(44, 186, 151),
                "102" => RGB::new(118, 90, 165),
                "103" => RGB::new(242, 131, 179),
                "111" => RGB::new(39, 127, 195),
                "112" => RGB::new(23, 176, 80),
                "131" => RGB::new(158, 120, 89),
                "151" => RGB::new(249, 165, 27),
                "171" => RGB::new(0, 151, 104),
                "172" => RGB::new(241, 86, 41),
                "173" => RGB::new(194, 56, 38),
                "174" => RGB::new(5, 98, 132),
                "182" => RGB::new(155, 43, 103),
                "191" => RGB::new(139, 157, 208),
                "192" => RGB::new(237, 72, 154),
                "405" => RGB::new(0, 181, 236),
                _ => fix_background_colour_rgb(background.clone()),
            }
        }
        "f-9q5-metro~losangeles" => match background == WHITE_RGB {
            true => RGB::new(225, 103, 16),
            false => {
                let metroid = &route.id.split('-').collect::<Vec<&str>>()[0];

                match metroid.len() == 3 && metroid.chars().nth(0).unwrap() == '7' {
                    true => RGB::new(209, 18, 66),
                    false => fix_background_colour_rgb(background.clone()),
                }
            }
        },
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
            _ => fix_background_colour_rgb(background.clone()),
        },
        _ => fix_background_colour_rgb(background.clone()),
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
    feed_id: &str,
    background: RGB<u8>,
    foreground: RGB<u8>,
) -> RGB<u8> {
    match feed_id {
        "f-9q5b-longbeachtransit" => WHITE_RGB,
        _ => fix_foreground_colour_rgb(background, foreground),
    }
}
