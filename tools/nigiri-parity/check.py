#!/usr/bin/env python3
"""Compare production Rust Dial/Dijkstra with the pinned upstream C++ headers.

Requires a Nigiri checkout with its pkg dependencies populated, g++, and rustc.
This checks the generic static queue/traversal, NOT full RAPTOR/PONG equivalence.
"""
import argparse
import json
import os
from pathlib import Path
import random
import shutil
import subprocess
import tempfile

PIN = '82e55cedfc184ea3a33082619ba937d172e9c575'
ROOT = Path(__file__).resolve().parents[2]

CPP = r'''
#include <cstdint>
#include <iostream>
#include <limits>
#include <utility>
#include <vector>
#include "nigiri/common/dial.h"
#include "nigiri/routing/dijkstra.h"
struct Label { using dist_t = std::uint32_t; nigiri::location_idx_t l_; dist_t d_; };
struct GetBucket { std::uint32_t operator()(Label const& x) const { return x.d_; } };
struct Edge {
  nigiri::location_idx_t to_;
  std::uint32_t seconds_;
  auto target() const { return to_; }
  auto duration() const { return std::chrono::seconds{seconds_}; }
};
int main() {
  char mode;
  std::cin >> mode;
  if (mode == 'Q') {
    unsigned horizon, count;
    std::cin >> horizon >> count;
    auto get = [](auto const& x) { return x.first; };
    nigiri::dial<std::pair<unsigned, unsigned>, decltype(get)> pq(horizon, get);
    while (count--) {
      char op;
      std::cin >> op;
      if (op == '+') {
        unsigned key, id;
        std::cin >> key >> id;
        pq.push(std::pair{key, id});
      } else if (pq.empty()) {
        std::cout << "none\n";
      } else {
        auto const [key, id] = pq.top();
        std::cout << key << ' ' << id << '\n';
        pq.pop();
      }
    }
  } else {
    unsigned n, m, horizon, max_dist, seeds;
    std::cin >> n >> m >> horizon >> max_dist >> seeds;
    std::vector<std::vector<Edge>> rows(n);
    for (unsigned i = 0; i != m; ++i) {
      unsigned from, to, cost;
      std::cin >> from >> to >> cost;
      rows[from].push_back({nigiri::location_idx_t{to}, cost});
    }
    nigiri::vecvec<nigiri::location_idx_t, Edge> graph;
    for (auto const& row : rows) { graph.emplace_back(row); }
    std::vector<std::uint32_t> distances(n, std::numeric_limits<std::uint32_t>::max());
    nigiri::dial<Label, GetBucket> pq(horizon);
    while (seeds--) {
      unsigned stop, distance;
      std::cin >> stop >> distance;
      distances[stop] = std::min(distance, distances[stop]);
      pq.push(Label{nigiri::location_idx_t{stop}, distance});
    }
    using Graph = decltype(graph);
    nigiri::routing::dijkstra(graph,
        static_cast<nigiri::bitvec_map<nigiri::location_idx_t> const*>(nullptr),
        static_cast<Graph const*>(nullptr), pq, distances, max_dist);
    for (auto d : distances) { std::cout << d << ' '; }
    std::cout << '\n';
  }
}
'''

RUST = r'''
#[path = __DIAL__]
mod dial;
use dial::{Dial, dijkstra};
use std::io::Read;
fn main() {
    let mut input = String::new();
    std::io::stdin().read_to_string(&mut input).unwrap();
    let mut words = input.split_whitespace();
    let mode = words.next().unwrap();
    if mode == "Q" {
        let horizon: usize = words.next().unwrap().parse().unwrap();
        let count: usize = words.next().unwrap().parse().unwrap();
        let mut queue = Dial::new(horizon, |x: &(u32, usize)| x.0 as usize);
        for _ in 0..count {
            if words.next().unwrap() == "+" {
                let key = words.next().unwrap().parse().unwrap();
                let id = words.next().unwrap().parse().unwrap();
                queue.push((key, id));
            } else {
                match queue.pop() {
                    Some((key, id)) => println!("{key} {id}"),
                    None => println!("none"),
                }
            }
        }
    } else {
        let n: usize = words.next().unwrap().parse().unwrap();
        let m: usize = words.next().unwrap().parse().unwrap();
        let horizon: usize = words.next().unwrap().parse().unwrap();
        let max_distance = words.next().unwrap().parse().unwrap();
        let seeds: usize = words.next().unwrap().parse().unwrap();
        let mut rows = vec![Vec::new(); n];
        for _ in 0..m {
            let from: usize = words.next().unwrap().parse().unwrap();
            let to = words.next().unwrap().parse().unwrap();
            let cost = words.next().unwrap().parse().unwrap();
            rows[from].push((to, cost));
        }
        let mut offsets = vec![0];
        let mut edges = Vec::new();
        for row in rows {
            edges.extend(row);
            offsets.push(edges.len());
        }
        let mut distances = vec![u32::MAX; n];
        let mut queue = Dial::new(horizon, |x: &(u32, usize)| x.0 as usize);
        for _ in 0..seeds {
            let stop: usize = words.next().unwrap().parse().unwrap();
            let distance: u32 = words.next().unwrap().parse().unwrap();
            distances[stop] = distance.min(distances[stop]);
            queue.push((distance, stop));
        }
        let _work = dijkstra(&offsets, &edges, &mut queue, &mut distances, max_distance);
        for distance in distances { print!("{distance} "); }
        println!();
    }
}
'''


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--nigiri', required=True, type=Path)
    parser.add_argument('--pong-oracle', type=Path, help='Native pong_oracle.cc executable linked against pinned Nigiri')
    args = parser.parse_args()
    upstream = args.nigiri.resolve()
    pin = subprocess.check_output(['git', '-C', str(upstream), 'rev-parse', 'HEAD'], text=True).strip()
    if pin != PIN:
        raise SystemExit(f'Expected Nigiri {PIN}, found {pin}')
    subprocess.run(['git', '-C', str(upstream), 'diff', '--exit-code', 'HEAD', '--',
                    'include/nigiri/common/dial.h', 'include/nigiri/routing/dijkstra.h',
                    'include/nigiri/routing/raptor/raptor.h', 'src/routing/raptor/pong.cc'],
                   check=True)
    compiler = shutil.which('g++')
    rustc = shutil.which('rustc') or str(Path.home() / '.cargo/bin/rustc')
    if compiler is None:
        raise SystemExit('g++ is required')
    with tempfile.TemporaryDirectory(prefix='nigiri-parity-') as directory:
        tmp = Path(directory)
        cpp = tmp / 'oracle.cc'
        rust = tmp / 'oracle.rs'
        cpp.write_text(CPP)
        rust.write_text(RUST.replace('__DIAL__', json.dumps(str(ROOT / 'src/routing_common/dial.rs'))))
        includes = [upstream / 'include', *sorted((upstream / 'deps').glob('*/include'))]
        subprocess.run([compiler, '-std=c++23', '-O2', '-DFMT_HEADER_ONLY=1',
                        *[f'-I{path}' for path in includes], str(cpp), '-o', str(tmp / 'cpp')], check=True)
        subprocess.run([rustc, '--edition=2024', '-O', str(rust), '-o', str(tmp / 'rust')], check=True)

        def compare(data, label):
            expected = subprocess.check_output([str(tmp / 'cpp')], input=data, text=True)
            actual = subprocess.check_output([str(tmp / 'rust')], input=data, text=True)
            if actual != expected:
                raise AssertionError(f'{label}:\nINPUT\n{data}\nCPP\n{expected}\nRUST\n{actual}')

        rng = random.Random(20261105)
        operations = []
        for i in range(10_000):
            operations.append(f'+ {rng.randrange(101)} {i}' if rng.random() < 0.55 else '-')
        operations.extend('-' for _ in range(10_000))
        compare('Q 100 ' + str(len(operations)) + '\n' + '\n'.join(operations) + '\n', 'Dial operation trace')
        for case in range(300):
            n = rng.randrange(1, 50)
            horizon = rng.choice([0, 1, 20, 100, 7200])
            max_dist = rng.choice([0, 1, 5, 77, 2**32 - 1])
            edges = [(rng.randrange(n), rng.randrange(n), rng.randrange(10_000))
                     for _ in range(rng.randrange(200))]
            seeds = [(rng.randrange(n), rng.randrange(horizon + 1))
                     for _ in range(rng.randrange(10))]
            data = f'G {n} {len(edges)} {horizon} {max_dist} {len(seeds)}\n'
            data += ''.join(f'{a} {b} {c}\n' for a, b, c in edges)
            data += ''.join(f'{a} {b}\n' for a, b in seeds)
            compare(data, f'directed Dijkstra graph {case}')
        print(f'PASS: upstream {PIN}; 20,000 Dial operations and 300 static Dijkstra cases match.')
        if args.pong_oracle:
            output = tmp / 'rust-pong.txt'
            cargo = shutil.which('cargo') or str(Path.home() / '.cargo/bin/cargo')

            def compare_pong(input_path=None):
                command = [str(args.pong_oracle.resolve())]
                env = os.environ.copy()
                env['CATENARY_NIGIRI_PARITY_OUTPUT'] = str(output)
                env.pop('CATENARY_NIGIRI_PARITY_INPUT', None)
                if input_path:
                    command.append(str(input_path))
                    env['CATENARY_NIGIRI_PARITY_INPUT'] = str(input_path)
                expected = subprocess.check_output(command, text=True)
                subprocess.run([cargo, 'test', '--locked', '--manifest-path',
                                str(ROOT / 'tools/raptor-check/Cargo.toml'),
                                'minute_aligned_pong_fixtures_for_native_comparison'], env=env, check=True)
                actual = output.read_text()
                if actual != expected:
                    raise AssertionError(f'PONG fixture mismatch:\nCPP\n{expected}\nRUST\n{actual}')

            compare_pong()
            print('PASS: native CPU PONG journey frontiers match on 5 minute-aligned fixtures in both directions.')
            cases = [([(0,1,7*3600,10*60),(1,2,7*3600+12*60,10*60),
                       (2,3,7*3600+25*60,10*60),(0,3,6*3600+50*60,60*60)],
                       5*3600,16*3600,900,2,0,0)]
            for _ in range(80):
                n = rng.randrange(3, 6)
                trips = []
                for _ in range(rng.randrange(10, 30)):
                    origin = rng.randrange(n)
                    destination = rng.randrange(n-1)
                    if destination >= origin:
                        destination += 1
                    trips.append((origin,destination,rng.randrange(6*60,12*60)*60,rng.randrange(5,121)*60))
                trips.append((0,n-1,11*3600,30*60))
                # Queries outside the service window isolate frontier parity
                # from the explicitly different second/minute cursor units.
                cases.append((trips,5*3600,16*3600,rng.choice([180,900]),rng.randrange(1,6),
                              rng.choice([0,120]),rng.choice([0,180])))
            fixture_path = tmp / 'generated-pong.txt'
            data = str(len(cases)) + '\n'
            for trips, forward, backward, window, minimum, access, egress in cases:
                data += f'{len(trips)} {forward} {backward} {window} {minimum} {access} {egress}\n'
                data += ''.join(f'{a} {b} {c} {d}\n' for a,b,c,d in trips)
            fixture_path.write_text(data)
            compare_pong(fixture_path)
            print('PASS: native CPU PONG journey frontiers match on 81 three-ride/generated networks in both directions.')
            print('Scope: shared static/no-via fixtures only; not all inputs, cursor units, RTT, or loader parity.')
        else:
            print('Scope: queue and generic static Dijkstra only; not RAPTOR/PONG, RTT, or loader parity.')



if __name__ == '__main__':
    main()
