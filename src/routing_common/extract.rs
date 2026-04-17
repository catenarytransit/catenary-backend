use crate::routing_common::types::*;
use crate::routing_common::ways::*;
use std::collections::HashMap;

#[derive(Default)]
pub struct OsmTags {
    pub is_route: bool,
    pub is_ferry_route: bool,
    pub oneway: bool,
    pub not_oneway_bike: bool,
    pub not_oneway_bus_psv: bool,
    pub barrier: Option<String>,
    pub motor_vehicle: Option<String>,
    pub motorcar: Option<String>,
    pub foot: Option<String>,
    pub bicycle: Option<String>,
    pub highway: Option<String>,
    pub railway: Option<String>,
    pub sidewalk_separate: bool,
    pub max_speed: Option<String>,
    pub name: Option<String>,
    pub is_destination: bool,
    pub vehicle_override: Option<bool>,
    pub access_override: Option<bool>,
    pub private_access: bool,
    pub bus_override: Option<bool>,
    pub is_parking: bool,
    pub is_elevator: bool,
    pub is_entrance: bool,
    pub is_platform: bool,
    pub is_ramp: bool,
    pub has_level: bool,
    pub is_incline_down: bool,
    pub toll: bool,
}

impl OsmTags {
    pub fn from_pbf(tags: &osmpbfreader::Tags) -> Self {
        let mut t = OsmTags::default();
        let mut circular = false;
        let mut oneway_defined = false;

        for (key, val) in tags.iter() {
            match key.as_str() {
                "ramp" => t.is_ramp |= val != "no",
                "type" => t.is_route |= val == "route",
                "parking" => t.is_parking = true,
                "amenity" => t.is_parking |= val == "parking" || val == "parking_entrance",
                "building" => t.is_parking |= val == "parking",
                "railway" => t.railway = Some(val.to_string()),
                "oneway" => {
                    oneway_defined = true;
                    t.oneway |= val == "yes";
                }
                "junction" => {
                    t.oneway |= val == "roundabout";
                    circular |= val == "circular";
                }
                "oneway:bicycle" => t.not_oneway_bike = val == "no",
                "oneway:bus" | "oneway:psv" => t.not_oneway_bus_psv |= val == "no",
                "motor_vehicle:forward" | "motor_vehicle" => {
                    t.motor_vehicle = Some(val.to_string());
                    t.is_destination |= val == "destination";
                }
                "foot" => t.foot = Some(val.to_string()),
                "bicycle" => t.bicycle = Some(val.to_string()),
                "highway" => {
                    t.highway = Some(val.to_string());
                    if val == "elevator" {
                        t.is_elevator = true;
                    }
                    if val == "bus_stop" {
                        t.is_platform = true;
                    }
                }
                "indoor:level" | "level" => {
                    t.has_level = true;
                }
                "name" => t.name = Some(val.to_string()),
                "entrance" => t.is_entrance = true,
                "sidewalk" | "sidewalk:both" | "sidewalk:left" | "sidewalk:right" => {
                    if val == "separate" {
                        t.sidewalk_separate = true;
                    }
                }
                "motorcar" => {
                    t.motorcar = Some(val.to_string());
                    t.is_destination |= val == "destination";
                }
                "barrier" => t.barrier = Some(val.to_string()),
                "platform_edge" => t.is_platform = true,
                "public_transport" => {
                    if val == "platform" || val == "stop_position" {
                        t.is_platform = true;
                    }
                }
                "vehicle" => match val.as_str() {
                    "private" | "delivery" | "no" => t.vehicle_override = Some(false),
                    "destination" => {
                        t.is_destination = true;
                        t.vehicle_override = Some(true);
                    }
                    "permissive" | "yes" => t.vehicle_override = Some(true),
                    _ => {}
                },
                "psv" => {
                    if t.bus_override.is_none() {
                        t.bus_override = Some(val != "no");
                    }
                }
                "bus" => t.bus_override = Some(val != "no"),
                "access" => match val.as_str() {
                    "no" | "agricultural" | "forestry" | "emergency" | "delivery" => {
                        t.access_override = Some(false);
                    }
                    "private" => {
                        t.access_override = Some(false);
                        t.private_access = true;
                    }
                    "designated" | "dismount" | "customers" | "permissive" | "yes" => {
                        t.access_override = Some(true);
                    }
                    "psv" | "bus" => {
                        t.access_override = Some(false);
                        t.bus_override = Some(true);
                    }
                    _ => {}
                },
                "maxspeed" => t.max_speed = Some(val.to_string()),
                "toll" => t.toll = val == "yes",
                "incline" => t.is_incline_down = val == "down" || val.starts_with("-"),
                "route" => t.is_ferry_route |= val == "ferry",
                _ => {}
            }
        }

        if circular && !oneway_defined {
            t.oneway = true;
        }

        t
    }
}

pub fn is_accessible_foot(t: &OsmTags, is_node: bool) -> bool {
    if t.is_route || t.sidewalk_separate || t.is_ferry_route {
        return false;
    }
    if let Some(ref b) = t.barrier {
        match b.as_str() {
            "yes" | "wall" | "fence" => return false,
            _ => {}
        }
    }
    if let Some(ref f) = t.foot {
        match f.as_str() {
            "no" | "private" | "use_sidepath" => return false,
            "yes" | "permissive" | "designated" => return true,
            _ => {}
        }
    }
    if t.is_platform || t.is_parking {
        return true;
    }
    if t.access_override == Some(false) {
        return false;
    }

    if !is_node {
        if t.is_elevator || t.is_parking {
            return true;
        }
        if let Some(ref h) = t.highway {
            match h.as_str() {
                "primary" | "primary_link" | "secondary" | "secondary_link" | "tertiary"
                | "tertiary_link" | "unclassified" | "residential" | "road" | "living_street"
                | "service" | "track" | "path" | "steps" | "pedestrian" | "platform"
                | "corridor" | "footway" | "pier" => return true,
                _ => return false,
            }
        }
        false
    } else {
        true
    }
}

pub fn is_accessible_car(t: &OsmTags, is_node: bool) -> bool {
    if t.access_override == Some(false)
        || t.is_route
        || t.is_ferry_route
        || (!is_node && t.highway.is_none())
    {
        return false;
    }
    if t.access_override == Some(true) {
        return true;
    }
    if let Some(ref b) = t.barrier {
        match b.as_str() {
            "cattle_grid" | "border_control" | "toll_booth" | "sally_port" | "gate"
            | "lift_gate" | "no" | "entrance" | "coupure" | "height_restrictor" | "arch" => {}
            _ => return false,
        }
    }

    let check_override = |val: Option<&String>| -> Option<bool> {
        match val?.as_str() {
            "private"
            | "optional_sidepath"
            | "agricultural"
            | "forestry"
            | "agricultural;forestry"
            | "permit"
            | "customers"
            | "delivery"
            | "no" => Some(false),
            "designated" | "permissive" | "yes" => Some(true),
            _ => None,
        }
    };

    if let Some(mv) = check_override(t.motor_vehicle.as_ref()) {
        return mv;
    }
    if let Some(mc) = check_override(t.motorcar.as_ref()) {
        return mc;
    }

    if t.is_parking {
        return true;
    }
    if let Some(vo) = t.vehicle_override {
        return vo;
    }

    if !is_node {
        if let Some(ref h) = t.highway {
            match h.as_str() {
                "motorway" | "motorway_link" | "trunk" | "trunk_link" | "primary"
                | "primary_link" | "secondary" | "secondary_link" | "tertiary"
                | "tertiary_link" | "residential" | "living_street" | "unclassified"
                | "service" => return true,
                _ => return false,
            }
        }
        false
    } else {
        true
    }
}

pub fn is_accessible_bus(t: &OsmTags, is_node: bool) -> bool {
    if !is_node && t.highway.is_none() {
        return false;
    } else if let Some(bo) = t.bus_override {
        return bo;
    } else if t.access_override == Some(true) {
        return true;
    } else if t.access_override == Some(false) || t.is_route || t.is_ferry_route {
        return false;
    } else if t.barrier.as_deref() == Some("bus_trap") {
        return true;
    }

    if t.highway.as_deref() == Some("busway") {
        return true;
    }

    is_accessible_car(t, is_node)
}

pub fn is_accessible_bus_penalty(t: &OsmTags, is_node: bool) -> bool {
    if is_node {
        if t.private_access {
            match t.barrier.as_deref() {
                Some("gate" | "lift_gate" | "swing_gate") => return true,
                _ => return false,
            }
        }
        false
    } else {
        t.private_access
            && (t.highway.as_deref() == Some("busway") || is_accessible_car(t, is_node))
    }
}

pub fn get_speed_limit(t: &OsmTags) -> SpeedLimit {
    if let Some(ref m) = t.max_speed {
        if let Ok(num) = m.parse::<u16>() {
            return SpeedLimit::from_kmh((num as f32 * 0.9) as u16);
        }
    }
    if let Some(ref h) = t.highway {
        match h.as_str() {
            "motorway" => return SpeedLimit::Kmh100,
            "motorway_link" => return SpeedLimit::Kmh50,
            "trunk" => return SpeedLimit::Kmh80,
            "trunk_link" => return SpeedLimit::Kmh50,
            "primary" => {
                return if t.name.is_none() {
                    SpeedLimit::Kmh80
                } else {
                    SpeedLimit::Kmh50
                };
            }
            "primary_link" => return SpeedLimit::Kmh30,
            "secondary" => {
                return if t.name.is_none() {
                    SpeedLimit::Kmh80
                } else {
                    SpeedLimit::Kmh60
                };
            }
            "secondary_link" => return SpeedLimit::Kmh30,
            "tertiary" => {
                return if t.name.is_none() {
                    SpeedLimit::Kmh60
                } else {
                    SpeedLimit::Kmh50
                };
            }
            "tertiary_link" => return SpeedLimit::Kmh20,
            "unclassified" => return SpeedLimit::Kmh50,
            "residential" => return SpeedLimit::Kmh30,
            "living_street" => return SpeedLimit::Kmh10,
            "service" => return SpeedLimit::Kmh20,
            "track" => return SpeedLimit::Kmh10,
            "path" => return SpeedLimit::Kmh10,
            "busway" => return SpeedLimit::Kmh50,
            _ => {}
        }
    }
    if let Some(ref r) = t.railway {
        match r.as_str() {
            "rail" | "narrow_gauge" => return SpeedLimit::Kmh80,
            "light_rail" | "subway" => return SpeedLimit::Kmh50,
            "tram" => return SpeedLimit::Kmh30,
            _ => {}
        }
    }
    SpeedLimit::Kmh10
}

pub fn is_big_street(t: &OsmTags) -> bool {
    if let Some(ref h) = t.highway {
        matches!(
            h.as_str(),
            "motorway"
                | "motorway_link"
                | "trunk"
                | "trunk_link"
                | "primary"
                | "primary_link"
                | "secondary"
                | "secondary_link"
                | "tertiary"
                | "tertiary_link"
                | "unclassified"
        )
    } else {
        false
    }
}

pub fn get_way_properties(t: &OsmTags) -> WayProperties {
    let mut builder = WayPropertiesBuilder::new();
    builder = builder
        .foot_accessible(is_accessible_foot(t, false))
        .car_accessible(is_accessible_car(t, false))
        .bus_accessible(is_accessible_bus(t, false))
        .bus_accessible_with_penalty(is_accessible_bus_penalty(t, false))
        .destination(t.is_destination)
        .oneway_car(t.oneway)
        .oneway_bike(t.oneway && !t.not_oneway_bike)
        .oneway_bus_psv(t.oneway && !t.not_oneway_bus_psv)
        .elevator(t.is_elevator)
        .steps(t.highway.as_deref() == Some("steps"))
        .parking(t.is_parking)
        .speed_limit(get_speed_limit(t))
        .incline_down(t.is_incline_down)
        .ramp(t.is_ramp)
        .sidewalk_separate(t.sidewalk_separate)
        .has_toll(t.toll)
        .big_street(is_big_street(t));

    // Just handling rail/ferry simply for now to cover tests
    builder = builder.ferry_accessible(t.is_ferry_route);
    if let Some(ref r) = t.railway {
        if matches!(
            r.as_str(),
            "rail" | "light_rail" | "monorail" | "narrow_gauge" | "subway" | "tram" | "funicular"
        ) {
            builder = builder.railway_accessible(true);
        }
    }

    builder.build()
}

pub fn get_node_properties(t: &OsmTags) -> NodeProperties {
    let mut builder = NodePropertiesBuilder::new();
    builder = builder
        .foot_accessible(is_accessible_foot(t, true))
        .car_accessible(is_accessible_car(t, true))
        .bus_accessible(is_accessible_bus(t, true))
        .bus_accessible_with_penalty(is_accessible_bus_penalty(t, true))
        .elevator(t.is_elevator)
        .entrance(t.is_entrance)
        .parking(t.is_parking);

    builder.build()
}

use memmap2::MmapMut;
use std::fs::{File, OpenOptions};
use std::io::{Read, Write};

/// Extract an OSM PBF file and build a complete disk-backed `RoutingGraph`.
pub fn extract_osm_graph(path: &str, out_path: &std::path::Path) {
    let file = std::fs::File::open(path).expect("failed to open pbf file");

    let num_max_nodes = 12_000_000_000usize;
    let map_size = num_max_nodes * 4;

    let temp_dir = std::env::temp_dir();
    let pid = std::process::id();
    let map_path = temp_dir.join(format!("avens_node_map_{}.bin", pid));
    let temp_ways_path = temp_dir.join(format!("avens_ways_{}.bin", pid));

    let map_file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .open(&map_path)
        .expect("Failed to create node map sparse file");

    map_file.set_len(map_size as u64).unwrap();
    let mut node_idx_map = unsafe { MmapMut::map_mut(&map_file).unwrap() };

    let mut ways_file = std::io::BufWriter::new(File::create(&temp_ways_path).unwrap());
    let mut tmp_ways_count = 0;

    let mut reader = osmpbfreader::OsmPbfReader::new(file);

    println!("Pass 1: Ways Collection...");
    for obj in reader.iter().filter_map(|o| o.ok()) {
        if let osmpbfreader::OsmObj::Way(way) = obj {
            let t = OsmTags::from_pbf(&way.tags);
            let p = get_way_properties(&t);

            // Pre-filtering optimization like motis
            if !t.is_elevator
                && !t.is_parking
                && (t.highway.is_none()
                    && t.railway.is_none()
                    && !t.is_ferry_route
                    && !t.is_platform)
            {
                continue;
            }

            if p.is_accessible() || t.is_ferry_route || t.is_route {
                let map_u32 = bytemuck::cast_slice_mut::<u8, u32>(&mut node_idx_map);
                for node_id in &way.nodes {
                    let id = node_id.0 as usize;
                    if id < num_max_nodes {
                        map_u32[id] = u32::MAX - 1;
                    }
                }

                let wp_bytes = bytemuck::bytes_of(&p);
                ways_file.write_all(wp_bytes).unwrap();
                let nodes_len = way.nodes.len() as u32;
                ways_file.write_all(bytemuck::bytes_of(&nodes_len)).unwrap();
                for n in &way.nodes {
                    ways_file.write_all(bytemuck::bytes_of(&n.0)).unwrap();
                }
                tmp_ways_count += 1;
            }
        }
    }
    ways_file.flush().unwrap();

    let file = std::fs::File::open(path).expect("failed to open pbf file");
    let mut reader = osmpbfreader::OsmPbfReader::new(file);
    let mut graph_builder = RoutingGraphBuilder::new(out_path);

    println!("Pass 2: Node Positions & Indexing...");
    for obj in reader.iter().filter_map(|o| o.ok()) {
        if let osmpbfreader::OsmObj::Node(node) = obj {
            let id = node.id.0 as usize;
            if id < num_max_nodes {
                let map_u32 = bytemuck::cast_slice_mut::<u8, u32>(&mut node_idx_map);
                if map_u32[id] == u32::MAX - 1 {
                    let t = OsmTags::from_pbf(&node.tags);
                    let np = get_node_properties(&t);
                    let pos = Point::from_latlng(node.lat(), node.lon());
                    let idx = graph_builder.add_node(pos, np);
                    // store (NodeIdx + 1) to avoid colliding with default unmapped 0
                    map_u32[id] = idx.0 + 1;
                }
            }
        }
    }

    println!("Pass 3: Finalizing Ways...");
    use std::io::BufReader;
    let mut ways_buf = BufReader::new(File::open(&temp_ways_path).unwrap());

    // Immutable view of map
    let map_u32 = bytemuck::cast_slice::<u8, u32>(&node_idx_map);

    for _ in 0..tmp_ways_count {
        let mut wp_bytes = [0u8; std::mem::size_of::<WayProperties>()];
        ways_buf.read_exact(&mut wp_bytes).unwrap();
        let p: WayProperties = *bytemuck::from_bytes(&wp_bytes);

        let mut len_bytes = [0u8; 4];
        ways_buf.read_exact(&mut len_bytes).unwrap();
        let nodes_len = u32::from_ne_bytes(len_bytes);

        let mut final_nodes = Vec::new();
        for _ in 0..nodes_len {
            let mut id_bytes = [0u8; 8];
            ways_buf.read_exact(&mut id_bytes).unwrap();
            let id = i64::from_ne_bytes(id_bytes) as usize;

            if id < num_max_nodes {
                let nidx_val = map_u32[id];
                if nidx_val > 0 && nidx_val < u32::MAX - 1 {
                    final_nodes.push(NodeIdx(nidx_val - 1));
                }
            }
        }

        if final_nodes.len() < 2 {
            continue;
        }

        let mut final_dists = Vec::with_capacity(final_nodes.len() - 1);
        for i in 0..final_nodes.len() - 1 {
            let n1 = final_nodes[i];
            let n2 = final_nodes[i + 1];
            let p1 = graph_builder.get_node_pos(n1);
            let p2 = graph_builder.get_node_pos(n2);
            let dist = p1.haversine_distance(&p2).round() as u16;
            final_dists.push(dist);
        }

        graph_builder.add_way(p, final_nodes, final_dists);
    }

    println!("Writing Graph arrays to routing.bin...");
    graph_builder.build();

    // cleanup
    std::fs::remove_file(&map_path).ok();
    std::fs::remove_file(&temp_ways_path).ok();
}
