use rgb::RGB;

pub const WHITE_RGB: RGB<u8> = RGB::new(255, 255, 255);

pub const DEFAULT_BACKGROUND: RGB<u8> = RGB::new(14, 165, 233);

pub fn fix_background_colour_rgb(background: Option<RGB<u8>>) -> RGB<u8> {
    if background.is_none()
        || background == Some(RGB::new(255, 255, 255))
        || background == Some(RGB::new(0, 0, 0))
    {
        DEFAULT_BACKGROUND
    } else {
        background.unwrap_or_else(|| DEFAULT_BACKGROUND)
    }
}

pub fn fix_background_colour_rgb_feed_route(
    feed_id: &str,
    background: Option<RGB<u8>>,
    route: &gtfs_structures::Route,
) -> RGB<u8> {
    match feed_id {
        "f-bus~dft~gov~uk" => match &route.short_name {
            Some(short_name) => match short_name.as_str() {
                "Bakerloo" => RGB::new(166, 90, 42),
                "Central" => RGB::new(225, 37, 27),
                "Circle" => RGB::new(255, 205, 0),
                "District" => RGB::new(20, 121, 52),
                "Hammersmith & City" => RGB::new(236, 155, 173),
                "Jubilee" => RGB::new(123, 134, 140),
                "Metropolitan" => RGB::new(135, 15, 84),
                "Northern" => RGB::new(0, 0, 0),
                "Piccadilly" => RGB::new(0, 15, 159),
                "Victoria" => RGB::new(0, 160, 223),
                "Waterloo & City" => RGB::new(107, 205, 178),
                _ => fix_background_colour_rgb(background),
            },

            _ => fix_background_colour_rgb(background),
        },
        "f-ez-renfeoperadora" => match route.short_name.as_deref() {
            Some("AVE") => RGB::new(214, 5, 95),
            _ => RGB::new(184, 183, 189),
        },
        "f-sf~bay~area~rg" => match route.id.as_str() {
            "CE:ACETrain" => RGB::new(195, 22, 204),
            _ => fix_background_colour_rgb(background),
        },
        "f-alhambra~ca~us" => match route.id.as_str() {
            "GreenLine" => RGB::new(88, 140, 50),
            _ => fix_background_colour_rgb(background),
        },
        "f-hongkong~justusewheels" => match route.id.as_str() {
            "MTR-TCL" => RGB::new(243, 152, 45),  // F3982D
            "MTR-TML" => RGB::new(156, 46, 0),    // 9C2E00
            "MTR-TWL" => RGB::new(230, 0, 18),    // E60012
            "MTR-TKL" => RGB::new(126, 60, 147),  // 7E3C93
            "MTR-ISL" => RGB::new(0, 117, 194),   // 0075C2
            "MTR-DRL" => RGB::new(235, 110, 165), // EB6EA5
            "MTR-KTL" => RGB::new(0, 160, 64),    // 00A040
            "MTR-EAL" => RGB::new(94, 183, 232),  // 5EB7E8
            "MTR-SIL" => RGB::new(203, 211, 0),   // CBD300
            "MTR-AEL" => RGB::new(0, 136, 142),   // 00888E
            _ => fix_background_colour_rgb(background),
        },
        "f-r6-nswtrainlink~sydneytrains~buswayswesternsydney~interlinebus" => {
            match &route.short_name {
                Some(short_name) => match &route.desc.as_deref() {
                    None => fix_background_colour_rgb(background),
                    Some("Sydney Buses Network") => match short_name.as_str() {
                        "114" => RGB::new(141, 198, 64),
                        "144" => RGB::new(238, 43, 123),
                        "254" => RGB::new(241, 170, 59),
                        "259" => RGB::new(0, 167, 158),
                        "267" => RGB::new(46, 53, 144),
                        "286" => RGB::new(156, 28, 35),
                        "287" => RGB::new(247, 148, 32),
                        "288" => RGB::new(241, 91, 43),
                        "290" => RGB::new(240, 78, 56),
                        "291" => RGB::new(18, 166, 79),
                        "292" => RGB::new(46, 53, 143),
                        "310" => RGB::new(187, 140, 191),
                        "320" => RGB::new(189, 32, 46),
                        "324" => RGB::new(12, 104, 56),
                        "324X" => RGB::new(122, 199, 133),
                        "325" => RGB::new(237, 31, 39),
                        "343" => RGB::new(31, 67, 152),
                        "389" => RGB::new(240, 78, 56),
                        "413" => RGB::new(19, 162, 74),
                        "422" => RGB::new(144, 40, 142),
                        "423" => RGB::new(18, 166, 79),
                        "423X" => RGB::new(46, 53, 144),
                        "426" => RGB::new(249, 160, 44),
                        "428" => RGB::new(235, 7, 140),
                        "430" => RGB::new(12, 104, 100),
                        "431" => RGB::new(58, 181, 74),
                        "433" => RGB::new(128, 64, 152),
                        "440" => RGB::new(40, 36, 96),
                        "480" => RGB::new(244, 154, 193),
                        "483" => RGB::new(237, 31, 39),
                        "461X" => RGB::new(247, 148, 32),
                        "500N" => RGB::new(102, 47, 145),
                        "500X" => RGB::new(115, 191, 68),
                        "501" => RGB::new(237, 31, 39),
                        "502" => RGB::new(11, 113, 185),
                        "503" => RGB::new(12, 104, 56),
                        "504" => RGB::new(115, 191, 68),
                        "504X" => RGB::new(187, 140, 191),
                        "505" => RGB::new(144, 40, 142),
                        "506" => RGB::new(12, 104, 56),
                        "517" => RGB::new(192, 40, 185),
                        "518" => RGB::new(0, 166, 79),
                        "544" => RGB::new(235, 9, 139),
                        "572" => RGB::new(248, 159, 110),
                        "575" => RGB::new(197, 25, 140),
                        "601" => RGB::new(94, 39, 145),
                        "603" => RGB::new(62, 125, 149),
                        "604" => RGB::new(10, 93, 102),
                        "608" => RGB::new(177, 40, 34),
                        "612X" => RGB::new(10, 64, 31),
                        "619" => RGB::new(175, 37, 109),
                        "626" => RGB::new(135, 129, 189),
                        "632" => RGB::new(237, 26, 82),
                        "633" => RGB::new(44, 57, 146),
                        "643" => RGB::new(236, 10, 109),
                        "651" => RGB::new(139, 94, 60),
                        "660" => RGB::new(19, 162, 74),
                        "662" => RGB::new(171, 190, 146),
                        "663" => RGB::new(246, 136, 21),
                        "665" => RGB::new(164, 140, 182),
                        "715" => RGB::new(203, 150, 27),
                        "731" => RGB::new(84, 194, 117),
                        "732" => RGB::new(134, 119, 195),
                        "735" => RGB::new(240, 64, 169),
                        "740" => RGB::new(177, 40, 34),
                        "746" => RGB::new(33, 89, 101),
                        "747" => RGB::new(171, 175, 32),
                        "748" => RGB::new(46, 52, 145),
                        "N20" => RGB::new(88, 90, 92),
                        "N50" => RGB::new(95, 57, 22),
                        "N61" => RGB::new(168, 123, 80),
                        "N81" => RGB::new(90, 74, 66),
                        "N92" => RGB::new(53, 10, 59),
                        _ => fix_background_colour_rgb(background),
                    },
                    Some("Hunter Buses Network") => match short_name.as_str() {
                        "10X" => RGB::new(46, 49, 146),
                        "12" => RGB::new(0, 146, 144),
                        "13" => RGB::new(247, 167, 27),
                        "14" => RGB::new(207, 31, 58),
                        "21" => RGB::new(68, 200, 245),
                        "22" => RGB::new(85, 190, 140),
                        "24" => RGB::new(240, 103, 166),
                        "25" => RGB::new(154, 197, 104),
                        "26" => RGB::new(135, 129, 189),
                        "27" => RGB::new(122, 149, 51),
                        "28" => RGB::new(146, 108, 87),
                        "29" => RGB::new(118, 112, 179),
                        "41" => RGB::new(110, 192, 93),
                        "43" => RGB::new(244, 133, 128),
                        "44" => RGB::new(58, 153, 139),
                        "47" => RGB::new(248, 159, 109),
                        "48" => RGB::new(82, 136, 181),
                        _ => fix_background_colour_rgb(background),
                    },
                    _ => fix_background_colour_rgb(background),
                },
                None => fix_background_colour_rgb(background),
            }
        }
        "f-9q5b-longbeachtransit" => {
            match route.id.as_str() {
                "1" => RGB::new(247, 161, 129),
                "2" => RGB::new(228, 228, 23),
                "8" => RGB::new(0, 167, 78),
                "22" => RGB::new(145, 7, 3),
                "23" => RGB::new(145, 7, 3),
                //passport
                "37" => RGB::new(214, 28, 41),
                "41" => RGB::new(219, 46, 24),
                "45" => RGB::new(0, 134, 172),
                "46" => RGB::new(225, 50, 29),
                "51" => RGB::new(227, 27, 140),
                "61" => RGB::new(12, 31, 107),
                "71" => RGB::new(4, 82, 161),
                "91" => RGB::new(193, 153, 6),
                "92" => RGB::new(193, 153, 6),
                "93" => RGB::new(193, 153, 6),
                "94" => RGB::new(193, 153, 6),
                "101" => RGB::new(0, 142, 136),
                "102" => RGB::new(0, 142, 136),
                "103" => RGB::new(0, 142, 136),
                "104" => RGB::new(0, 142, 136),
                "111" => RGB::new(39, 127, 195),
                "112" => RGB::new(23, 176, 80),
                "121" => RGB::new(54, 163, 198),
                "131" => RGB::new(158, 120, 89),
                "151" => RGB::new(249, 165, 27),
                "171" => RGB::new(116, 41, 142),
                "172" => RGB::new(116, 41, 142),
                "173" => RGB::new(116, 41, 142),
                "174" => RGB::new(116, 41, 142),
                "175" => RGB::new(116, 41, 142),
                "182" => RGB::new(155, 43, 103),
                "191" => RGB::new(2, 127, 61),
                "192" => RGB::new(2, 127, 61),
                "405" => RGB::new(0, 181, 236),
                _ => fix_background_colour_rgb(background),
            }
        }
        "f-9q5-metro~losangeles" => match background == None {
            true => RGB::new(225, 103, 16),
            false => {
                let metroid = &route.id.split('-').collect::<Vec<&str>>()[0];

                match metroid.len() == 3 && metroid.starts_with('7') {
                    true => RGB::new(209, 18, 66),
                    false => fix_background_colour_rgb(background),
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
            "235" => RGB::new(242, 0, 16),
            "215" => RGB::new(59, 192, 225),
            "225" => RGB::new(41, 52, 144),
            "227" => RGB::new(123, 194, 77),
            "510" => RGB::new(0, 112, 191),
            "520" => RGB::new(255, 143, 0),
            "530" => RGB::new(0, 171, 70),
            "535" => RGB::new(184, 115, 51),
            "888" | "891" | "892" | "894" => RGB::new(98, 54, 27),
            _ => RGB::new(220, 38, 38),
        },
        "f-c23-metrokingcounty" => match route.short_name.as_deref() {
            // RapidRide Red
            Some("A Line" | "B Line" | "C Line" | "D Line" | "E Line" | "F Line" | "H Line") => {
                RGB::new(180, 10, 54)
            }
            // Override default region-wide blue to separate from ST Express
            _ => RGB::new(41, 133, 107),
        },
        _ => fix_background_colour_rgb(background),
    }
}

pub fn fix_foreground_colour_rgb(
    background: Option<RGB<u8>>,
    foreground: Option<RGB<u8>>,
) -> RGB<u8> {
    if background == foreground {
        RGB::new(0, 0, 0)
    } else {
        foreground.unwrap_or_else(|| RGB::new(0, 0, 0))
    }
}

pub fn fix_foreground_colour_rgb_feed(
    feed_id: &str,
    background: Option<RGB<u8>>,
    foreground: Option<RGB<u8>>,
) -> RGB<u8> {
    match feed_id {
        "f-9q5b-longbeachtransit" => WHITE_RGB,
        _ => fix_foreground_colour_rgb(background, foreground),
    }
}
