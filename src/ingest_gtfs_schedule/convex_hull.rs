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

#[test]
// from https://codegolf.stackexchange.com/questions/11035/find-the-convex-hull-of-a-set-of-2d-points
fn lots_of_points() {
    let list = vec![
        (4.4, 14.),
        (6.7, 15.25),
        (6.9, 12.8),
        (2.1, 11.1),
        (9.5, 14.9),
        (13.2, 11.9),
        (10.3, 12.3),
        (6.8, 9.5),
        (3.3, 7.7),
        (0.6, 5.1),
        (5.3, 2.4),
        (8.45, 4.7),
        (11.5, 9.6),
        (13.8, 7.3),
        (12.9, 3.1),
        (11., 1.1),
    ];
    let ans = vec![
        (11., 1.1),
        (12.9, 3.1),
        (13.8, 7.3),
        (13.2, 11.9),
        (9.5, 14.9),
        (6.7, 15.25),
        (4.4, 14.),
        (2.1, 11.1),
        (0.6, 5.1),
        (5.3, 2.4),
    ];

    assert_eq!(convex_hull(&list), ans);
}