use crate::globeflower::loom_graph::*;
use std::cmp::Ordering;
use std::collections::{BTreeMap,BTreeSet,BinaryHeap,HashMap};

#[derive(Debug,Clone)]
pub struct TopoConfig {
    pub max_aggr_distance:f64,
    pub max_length_dev:f64,
    pub max_turn_restr_check_dist:f64,
    pub segment_length:f64,
    pub infer_restrictions:bool,
}
impl Default for TopoConfig {
    fn default()->Self { Self {
        max_aggr_distance:50.0, max_length_dev:500.0,
        max_turn_restr_check_dist:50.0, segment_length:5.0,
        infer_restrictions:true,
    }}
}

#[derive(Clone)]
struct Atom { orig:usize, line:LineOcc, a:Point, b:Point }

/// Clean-room Rust port of the Chapter-3/topo pipeline:
/// 1. remember station occurrences and original-edge provenance
/// 2. segment preliminary trip geometry at 5 m
/// 3. aggregate geometrically compatible segments inside 50 m
/// 4. construct the free line graph from the shared segment support
/// 5. infer line-specific forbidden turns from original connectivity
/// 6. reinsert stations using LOOM's served-edge/served-line scoring
pub fn run(mut input:Graph,cfg:&TopoConfig)->Graph {
    let station_occ=collect_stations(&mut input);
    let atoms=atomize(&input,cfg.segment_length);
    let mut out=aggregate(&input,&atoms,cfg.max_aggr_distance);
    if cfg.infer_restrictions { infer_restrictions(&input,&mut out,cfg); }
    insert_stations(&station_occ,&mut out,cfg);
    out
}

#[derive(Clone)]
struct StationOcc {
    stops:Vec<Stop>, originals:BTreeSet<usize>, lines:BTreeSet<LineId>, geom:Vec<Point>
}

fn collect_stations(g:&mut Graph)->Vec<StationOcc>{
    let mut by_name=BTreeMap::<String,StationOcc>::new();
    for n in g.nodes.iter_mut().filter_map(Option::as_mut) {
        for s in std::mem::take(&mut n.stops) {
            let ent=by_name.entry(s.name.clone()).or_insert_with(||StationOcc{
                stops:vec![],originals:BTreeSet::new(),lines:BTreeSet::new(),geom:vec![]
            });
            ent.geom.push(s.pos); ent.stops.push(s);
            for &eid in &n.adj {
                if let Some(e)=&g.edges[eid] {
                    ent.originals.extend(e.originals.iter().copied());
                    ent.lines.extend(e.lines.iter().map(|o|o.line));
                }
            }
        }
    }
    by_name.into_values().collect()
}

fn atomize(g:&Graph,step:f64)->Vec<Atom>{
    let mut out=vec![];
    for e in g.edges.iter().filter_map(Option::as_ref) {
        let dense=densify(&e.geom,step);
        for w in dense.windows(2) {
            for &line in &e.lines {
                for &orig in &e.originals { out.push(Atom{orig,line,a:w[0],b:w[1]}); }
            }
        }
    }
    out
}

fn midpoint(a:Point,b:Point)->Point{lerp(a,b,.5)}
fn bearing(a:Point,b:Point)->f64{
    let y=(b.lon-a.lon).to_radians()*((a.lat+b.lat)*.5).to_radians().cos();
    let x=(b.lat-a.lat).to_radians(); y.atan2(x)
}
fn angle_diff(a:f64,b:f64)->f64{
    let mut d=(a-b).abs()%std::f64::consts::PI;
    if d>std::f64::consts::FRAC_PI_2 {d=std::f64::consts::PI-d} d
}

struct Uf{p:Vec<usize>,r:Vec<u8>}
impl Uf{
 fn new(n:usize)->Self{Self{p:(0..n).collect(),r:vec![0;n]}}
 fn f(&mut self,x:usize)->usize{if self.p[x]!=x{let z=self.f(self.p[x]);self.p[x]=z}self.p[x]}
 fn u(&mut self,a:usize,b:usize){let(mut a,mut b)=(self.f(a),self.f(b));if a==b{return}if self.r[a]<self.r[b]{std::mem::swap(&mut a,&mut b)}self.p[b]=a;if self.r[a]==self.r[b]{self.r[a]+=1}}
}

/// Segment agglomeration. Candidate pairs must be near, nearly collinear and have
/// overlapping projections. Transitive closure gives LOOM-style shared segments.
fn aggregate(input:&Graph,atoms:&[Atom],maxd:f64)->Graph{
    let mut uf=Uf::new(atoms.len());
    // Spatial bucketing avoids O(n^2) globally.
    let cell=maxd/111_320.0;
    let mut buckets=HashMap::<(i64,i64),Vec<usize>>::new();
    for (i,a) in atoms.iter().enumerate(){
        let m=midpoint(a.a,a.b);
        let k=((m.lon/cell).floor() as i64,(m.lat/cell).floor() as i64);
        buckets.entry(k).or_default().push(i);
    }
    for (k,ids) in buckets.clone(){
        for dx in -1..=1 {for dy in -1..=1 {
            let Some(js)=buckets.get(&(k.0+dx,k.1+dy)) else{continue};
            for &i in &ids {for &j in js {
                if j<=i{continue}
                let a=&atoms[i];let b=&atoms[j];
                if haversine_m(midpoint(a.a,a.b),midpoint(b.a,b.b))>maxd{continue}
                if angle_diff(bearing(a.a,a.b),bearing(b.a,b.b))>35f64.to_radians(){continue}
                uf.u(i,j);
            }}
        }}
    }
    let mut groups=BTreeMap::<usize,Vec<usize>>::new();
    for i in 0..atoms.len(){let r=uf.f(i);groups.entry(r).or_default().push(i);}

    let mut g=Graph::default(); g.lines=input.lines.clone();
    // Snap endpoints of shared segments using a second 50 m union-find.
    let mut segs=vec![];
    for ids in groups.into_values(){
        let mut ax=0.;let mut ay=0.;let mut bx=0.;let mut by=0.;
        let ref_b=bearing(atoms[ids[0]].a,atoms[ids[0]].b);
        let mut los=BTreeSet::new();let mut orig=BTreeSet::new();
        for &i in &ids{
            let mut a=atoms[i].a;let mut b=atoms[i].b;
            if (bearing(a,b)-ref_b).cos()<0.0{std::mem::swap(&mut a,&mut b)}
            ax+=a.lon;ay+=a.lat;bx+=b.lon;by+=b.lat;
            los.insert(atoms[i].line);orig.insert(atoms[i].orig);
        }
        let n=ids.len() as f64;
        segs.push((Point{lon:ax/n,lat:ay/n},Point{lon:bx/n,lat:by/n},los,orig));
    }
    let mut endpoints=vec![];
    for s in &segs{endpoints.push(s.0);endpoints.push(s.1)}
    let mut eu=Uf::new(endpoints.len());
    for i in 0..endpoints.len(){for j in i+1..endpoints.len(){
        if haversine_m(endpoints[i],endpoints[j])<=maxd{eu.u(i,j)}
    }}
    let mut cent=HashMap::<usize,(f64,f64,usize)>::new();
    for i in 0..endpoints.len(){let r=eu.f(i);let e=cent.entry(r).or_insert((0.,0.,0));e.0+=endpoints[i].lon;e.1+=endpoints[i].lat;e.2+=1}
    let mut node_for=HashMap::new();
    for (r,(x,y,n)) in cent{node_for.insert(r,g.add_node(Point{lon:x/n as f64,lat:y/n as f64}));}
    for (si,s) in segs.into_iter().enumerate(){
        let a=node_for[&eu.f(2*si)];let b=node_for[&eu.f(2*si+1)];if a==b{continue}
        let eid=g.add_edge(a,b,vec![g.nodes[a].as_ref().unwrap().pos,g.nodes[b].as_ref().unwrap().pos]);
        let e=g.edges[eid].as_mut().unwrap();e.lines=s.2;e.originals=s.3;
    }
    contract_degree_two(&mut g);
    g
}

fn contract_degree_two(g:&mut Graph){
    loop{
        let cand=g.nodes.iter().enumerate().find_map(|(i,n)|{
            let n=n.as_ref()?; if n.adj.len()!=2||!n.stops.is_empty(){return None}
            let mut it=n.adj.iter();Some((i,*it.next().unwrap(),*it.next().unwrap()))
        });
        let Some((n,e1,e2))=cand else{break};
        let (Some(a),Some(b))=(g.edges[e1].clone(),g.edges[e2].clone()) else{continue};
        if a.lines!=b.lines{ // topology changes here; preserve junction
            g.nodes[n].as_mut().unwrap().adj.insert(e1); break
        }
        let u=if a.a==n{a.b}else{a.a};let v=if b.a==n{b.b}else{b.a};
        if u==v{break}
        let mut geom=a.geom.clone(); if *geom.last().unwrap()!=g.nodes[n].as_ref().unwrap().pos{geom.reverse()}
        let mut bg=b.geom.clone(); if bg[0]!=g.nodes[n].as_ref().unwrap().pos{bg.reverse()}
        geom.extend(bg.into_iter().skip(1));
        g.remove_edge(e1);g.remove_edge(e2);g.nodes[n]=None;
        let ne=g.add_edge(u,v,geom);let x=g.edges[ne].as_mut().unwrap();
        x.lines=a.lines;x.originals=a.originals.union(&b.originals).copied().collect();
    }
}

fn infer_restrictions(orig:&Graph,g:&mut Graph,cfg:&TopoConfig){
    // LOOM's semantic invariant: a turn is allowed for line L iff the preliminary
    // graph contains compatible original occurrences, or a short L-specific path
    // explains the construction displacement within max_length_dev.
    let nodes:Vec<usize>=g.nodes.iter().enumerate().filter(|(_,n)|n.is_some()).map(|x|x.0).collect();
    for n in nodes {
        let adj:Vec<usize>=g.nodes[n].as_ref().unwrap().adj.iter().copied().collect();
        for &ein in &adj {for &eout in &adj {
            if ein==eout{continue}
            let Some(a)=&g.edges[ein] else{continue};let Some(b)=&g.edges[eout] else{continue};
            let lines:BTreeSet<_>=a.lines.iter().map(|x|x.line).collect();
            for l in lines {
                if !b.lines.iter().any(|x|x.line==l){continue}
                let direct=a.originals.iter().any(|oa| b.originals.iter().any(|ob| orig_connected(orig,*oa,*ob,l)));
                if !direct && !short_explanation(g,ein,eout,l,cfg.max_length_dev) {
                    g.nodes[n].as_mut().unwrap().conn_exc.entry(l).or_default().entry(ein).or_default().insert(eout);
                }
            }
        }}
    }
}
fn orig_connected(g:&Graph,a:usize,b:usize,l:LineId)->bool{
    let ea=g.edges.iter().filter_map(Option::as_ref).find(|e|e.originals.contains(&a));
    let eb=g.edges.iter().filter_map(Option::as_ref).find(|e|e.originals.contains(&b));
    match(ea,eb){(Some(a),Some(b))=>{
        (a.a==b.a||a.a==b.b||a.b==b.a||a.b==b.b)&&a.lines.iter().any(|x|x.line==l)&&b.lines.iter().any(|x|x.line==l)
    },_=>false}
}
#[derive(Copy,Clone,PartialEq)]struct Q(f64,usize);
impl Eq for Q{} impl Ord for Q{fn cmp(&self,o:&Self)->Ordering{o.0.total_cmp(&self.0)}}impl PartialOrd for Q{fn partial_cmp(&self,o:&Self)->Option<Ordering>{Some(self.cmp(o))}}
fn short_explanation(g:&Graph,ein:usize,eout:usize,l:LineId,max:f64)->bool{
    let Some(a)=&g.edges[ein]else{return false};let Some(b)=&g.edges[eout]else{return false};
    let starts=[a.a,a.b];let goals=[b.a,b.b];
    let mut d=vec![f64::INFINITY;g.nodes.len()];let mut q=BinaryHeap::new();
    for s in starts{d[s]=0.;q.push(Q(0.,s))}
    while let Some(Q(cd,n))=q.pop(){if cd>d[n]||cd>max{continue}if goals.contains(&n){return true}
        let Some(nd)=&g.nodes[n]else{continue};
        for &eid in &nd.adj{let Some(e)=&g.edges[eid]else{continue};if !e.lines.iter().any(|x|x.line==l){continue}
            let v=if e.a==n{e.b}else{e.a};let nd=cd+polyline_len(&e.geom);if nd<d[v]{d[v]=nd;q.push(Q(nd,v))}
        }
    } false
}

fn insert_stations(occ:&[StationOcc],g:&mut Graph,cfg:&TopoConfig){
    for o in occ {
        let mut remaining=o.clone();
        for _ in 0..3 {
            let mut best:Option<(f64,usize,f64,BTreeSet<usize>,BTreeSet<LineId>)>=None;
            for e in g.edges.iter().filter_map(Option::as_ref){
                let (q,pos,dist)=project_on_polyline(remaining.stops[0].pos,&e.geom);
                if dist>4.0*cfg.max_aggr_distance{continue}
                let served_orig:BTreeSet<_>=e.originals.intersection(&remaining.originals).copied().collect();
                let edge_lines:BTreeSet<_>=e.lines.iter().map(|x|x.line).collect();
                let served_lines:BTreeSet<_>=edge_lines.intersection(&remaining.lines).copied().collect();
                let mut score=dist;
                if !remaining.originals.is_empty(){score+=(remaining.originals.len()-served_orig.len()) as f64/remaining.originals.len() as f64*100.0}
                if !remaining.lines.is_empty(){score+=(remaining.lines.len()-served_lines.len()) as f64/remaining.lines.len() as f64*500.0}
                if pos*polyline_len(&e.geom)<cfg.max_aggr_distance||(1.0-pos)*polyline_len(&e.geom)<cfg.max_aggr_distance{score+=200.0}
                if best.as_ref().map_or(true,|x|score<x.0){best=Some((score,e.id,pos,served_orig,served_lines))}
            }
            let Some((_score,eid,pos,so,sl))=best else{break};
            if so.is_empty()&&sl.is_empty(){break}
            let nid=split_edge(g,eid,pos);
            g.nodes[nid].as_mut().unwrap().stops.push(remaining.stops[0].clone());
            for l in &remaining.lines {g.nodes[nid].as_mut().unwrap().not_served.remove(l);}
            remaining.originals=remaining.originals.difference(&so).copied().collect();
            remaining.lines=remaining.lines.difference(&sl).copied().collect();
            if remaining.originals.is_empty()&&remaining.lines.is_empty(){break}
        }
    }
}
fn split_edge(g:&mut Graph,eid:usize,pos:f64)->usize{
    let e=g.edges[eid].clone().unwrap();let q=project_on_polyline(g.nodes[e.a].as_ref().unwrap().pos,&e.geom).0;
    let left=subline(&e.geom,0.,pos);let right=subline(&e.geom,pos,1.);
    let p=*left.last().unwrap_or(&lerp(g.nodes[e.a].as_ref().unwrap().pos,g.nodes[e.b].as_ref().unwrap().pos,pos));
    g.remove_edge(eid);let n=g.add_node(p);
    for (a,b,geom) in [(e.a,n,left),(n,e.b,right)] {
        let id=g.add_edge(a,b,geom);let x=g.edges[id].as_mut().unwrap();x.lines=e.lines.clone();x.originals=e.originals.clone();
    } n
}
