use std::cmp::Ordering::Equal;

pub fn convex_hull(input: &Vec<(f64, f64)>) -> Vec<(f64, f64)> {
    if input.len() <= 3 {
        return input.clone();
    }

    let mut stack: Vec<(f64, f64)> = vec![];

    let min = input
    .iter()
    .min_by(|a, b| {
        let ord = a.1.partial_cmp(&b.1).unwrap_or(Equal);

        match ord {
            Equal => a.0.partial_cmp(&b.0).unwrap_or(Equal),
            o => o,
        }
    })
    .unwrap();
    
    let points = sort_by_min_angle(&input, min);

    for point in points {
        while stack.len() > 1
            && calc_z_coord_vector_product(&stack[stack.len() - 2], &stack[stack.len() - 1], &point)
                < 0.
        {
            stack.pop();
        }
        stack.push(point);
    }

    stack
}

fn sort_by_min_angle(pts: &[(f64, f64)], min: &(f64, f64)) -> Vec<(f64, f64)> {
    let mut points: Vec<(f64, f64, (f64, f64))> = pts
        .iter()
        .map(|x| {
            (
                (x.1 - min.1).atan2(x.0 - min.0),
                // angle
                (x.1 - min.1).hypot(x.0 - min.0),
                // distance (we want the closest to be first)
                *x,
            )
        })
        .collect();
    points.sort_by(|a, b| a.partial_cmp(b).unwrap_or(Equal));
    points.into_iter().map(|x| x.2).collect()
    }

    // calculates the z coordinate of the vector product of vectors ab and ac
fn calc_z_coord_vector_product(a: &(f64, f64), b: &(f64, f64), c: &(f64, f64)) -> f64 {
    (b.0 - a.0) * (c.1 - a.1) - (c.0 - a.0) * (b.1 - a.1)
}