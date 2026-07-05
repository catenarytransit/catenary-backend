fn perpendicular_distance_sq(pt: [f64; 2], line_start: [f64; 2], line_end: [f64; 2]) -> f64 {
    let dx = line_end[0] - line_start[0];
    let dy = line_end[1] - line_start[1];
    
    let length_sq = dx * dx + dy * dy;
    if length_sq == 0.0 {
        let dx = pt[0] - line_start[0];
        let dy = pt[1] - line_start[1];
        return dx * dx + dy * dy;
    }

    let t = ((pt[0] - line_start[0]) * dx + (pt[1] - line_start[1]) * dy) / length_sq;
    let t = t.clamp(0.0, 1.0);

    let proj_x = line_start[0] + t * dx;
    let proj_y = line_start[1] + t * dy;

    let dx = pt[0] - proj_x;
    let dy = pt[1] - proj_y;
    
    dx * dx + dy * dy
}

pub fn rdp_indices(projected: &[[f64; 2]], epsilon_sq: f64) -> Vec<usize> {
    if projected.len() <= 2 {
        return (0..projected.len()).collect();
    }

    let mut stack = vec![(0, projected.len() - 1)];
    let mut keep = vec![false; projected.len()];
    keep[0] = true;
    keep[projected.len() - 1] = true;

    while let Some((start, end)) = stack.pop() {
        let mut dmax_sq = 0.0;
        let mut index = start;

        let pt_start = projected[start];
        let pt_end = projected[end];

        for i in start + 1..end {
            let d_sq = perpendicular_distance_sq(projected[i], pt_start, pt_end);
            if d_sq > dmax_sq {
                index = i;
                dmax_sq = d_sq;
            }
        }

        if dmax_sq > epsilon_sq {
            keep[index] = true;
            stack.push((start, index));
            stack.push((index, end));
        }
    }

    keep.into_iter().enumerate().filter_map(|(i, k)| if k { Some(i) } else { None }).collect()
}

pub fn web_mercator(lon: f64, lat: f64) -> [f64; 2] {
    let r = 6378137.0;
    let x = lon.to_radians() * r;
    let y = ((std::f64::consts::PI / 4.0) + (lat.to_radians() / 2.0)).tan().ln() * r;
    [x, y]
}

fn main() {
    let points = vec![
        [0.0, 0.0],
        [1.0, 0.0],
        [1.0, 1.0],
        [2.0, 1.0],
        [2.0, 0.0],
    ];
    let proj = points.iter().map(|p| web_mercator(p[0], p[1])).collect::<Vec<_>>();
    let indices = rdp_indices(&proj, 1.0);
    println!("{:?}", indices);
}
