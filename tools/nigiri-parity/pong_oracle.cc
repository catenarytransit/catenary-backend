// Native CPU PONG oracle for shared, minute-aligned, static/no-via fixtures.
// Gap fixture: Nigiri test/routing/pong_gap_truncation_test.cc at
// 82e55cedfc184ea3a33082619ba937d172e9c575 (third_party/nigiri-LICENSE).
#include <algorithm>
#include <chrono>
#include <iomanip>
#include <fstream>
#include <iostream>
#include <sstream>
#include <tuple>
#include <vector>
#include "utl/progress_tracker.h"
#include "nigiri/loader/dir.h"
#include "nigiri/loader/load.h"
#include "nigiri/routing/raptor/pong.h"
#include "nigiri/timetable.h"

using namespace nigiri;
using namespace nigiri::routing;
using namespace std::chrono_literals;
struct Trip { unsigned from, to, dep, duration; };
struct Fixture {
  std::vector<Trip> trips;
  unsigned forward, backward, window, minimum, access, egress;
};
std::string hhmm(unsigned seconds) {
  std::ostringstream out;
  out << std::setfill('0') << std::setw(2) << seconds/3600 << ':'
      << std::setw(2) << seconds/60%60 << ':' << std::setw(2) << seconds%60;
  return out.str();
}
std::string gtfs(Fixture const& fixture) {
  std::ostringstream out;
  unsigned n=3;
  for (auto const& t : fixture.trips) n=std::max(n,std::max(t.from,t.to)+1);
  out << R"(
# agency.txt
agency_id,agency_name,agency_url,agency_timezone
S,Test,https://example.com,Etc/UTC
# stops.txt
stop_id,stop_name,stop_lat,stop_lon
)";
  for (unsigned i=0; i<n; ++i) out << i << ",Stop" << i << ",0," << i << '\n';
  out << R"(
# calendar_dates.txt
service_id,date,exception_type
S,20241004,1
# routes.txt
route_id,agency_id,route_short_name,route_type
)";
  for (unsigned i=0; i<fixture.trips.size(); ++i) out << i << ",S,R" << i << ",2\n";
  out << "\n# trips.txt\nroute_id,service_id,trip_id\n";
  for (unsigned i=0; i<fixture.trips.size(); ++i) out << i << ",S," << i << '\n';
  out << "\n# stop_times.txt\ntrip_id,arrival_time,departure_time,stop_id,stop_sequence,pickup_type,drop_off_type\n";
  for (unsigned i=0; i<fixture.trips.size(); ++i) {
    auto const& t=fixture.trips[i];
    out << i << ',' << hhmm(t.dep) << ',' << hhmm(t.dep) << ',' << t.from << ",1,0,0\n";
    out << i << ',' << hhmm(t.dep+t.duration) << ',' << hhmm(t.dep+t.duration) << ',' << t.to << ",2,0,0\n";
  }
  return out.str();
}
int main(int argc, char** argv) {
  nigiri::s_verbosity=nigiri::log_lvl::error;
  auto const tracker=utl::activate_progress_tracker("parity");
  auto const silence=utl::global_progress_bars{true};
  std::vector<Fixture> fixtures{
    {{{0,2,8*3600,3600},{0,1,8*3600+10*60,10*60},{1,2,8*3600+25*60,5*60}},
      8*3600,9*3600,900,1,0,0},
    {{{0,2,9*3600+59*60,61*60},{0,1,10*3600+5*60,15*60},
      {1,2,10*3600+30*60,30*60},{0,2,15*3600,30*60}},10*3600,16*3600,180,2,0,0},
    {{{0,1,23*3600+55*60,35*60},{1,2,24*3600+35*60,20*60},
      {0,2,23*3600+50*60,90*60}},23*3600+40*60,25*3600+30*60,900,1,0,0},
    {{{0,2,8*3600,3600},{0,2,8*3600+10*60,50*60}},8*3600,9*3600,900,1,0,0},
    {{{0,2,8*3600,3600},{0,1,8*3600+10*60,10*60},{1,2,8*3600+25*60,5*60}},
      8*3600-120,9*3600+180,900,1,120,180},
  };
  if (argc>1) {
    std::ifstream input(argv[1]);
    unsigned count;
    input >> count;
    fixtures.clear();
    for (unsigned i=0; i<count; ++i) {
      unsigned n;
      Fixture f;
      input >> n >> f.forward >> f.backward >> f.window >> f.minimum >> f.access >> f.egress;
      while (n--) {
        Trip t;
        input >> t.from >> t.to >> t.dep >> t.duration;
        f.trips.push_back(t);
      }
      fixtures.push_back(std::move(f));
    }
  }
  auto const day=date::sys_days{date::year{2024}/10/4};
  for (unsigned case_id=0; case_id<fixtures.size(); ++case_id) {
    auto const& f=fixtures[case_id];
    auto tt=loader::load(loader::mem_dir::read(gtfs(f)), {day,day+date::days{2}});
    // Normalize the shared fixture: no station change time or nearby-stop
    // auto-footpaths. Stop coordinates are a full degree apart.
    for (auto& transfer : tt.locations_.transfer_time_) transfer=0min;
    auto const from=tt.find(location_id{"0",source_idx_t{0}}).value();
    unsigned n=3;
    for (auto const& t : f.trips) n=std::max(n,std::max(t.from,t.to)+1);
    auto const to=tt.find(location_id{std::to_string(n-1),source_idx_t{0}}).value();
    for (bool reverse : {false,true}) {
      auto const reference=day+std::chrono::minutes{(reverse?f.backward:f.forward)/60};
      auto const start=reverse?reference-std::chrono::minutes{f.window/60}:reference;
      auto const end=reverse?reference+1min:reference+std::chrono::minutes{f.window/60};
      auto q=query{
        .start_time_=interval<unixtime_t>{start,end},
        .start_match_mode_=location_match_mode::kIntermodal,
        .dest_match_mode_=location_match_mode::kIntermodal,
        .use_start_footpaths_=false,
        .start_={{from,std::chrono::minutes{f.access/60},0U}},
        .destination_={{to,std::chrono::minutes{f.egress/60},0U}},
        .max_transfers_=2,
        .max_travel_time_=4_days,
        .min_connection_count_=f.minimum,
        .extend_interval_earlier_=reverse,
        .extend_interval_later_=!reverse,
      };
      auto search=search_state{};
      auto state=raptor_state{};
      // Nigiri query endpoints are in search order. Catenary's API keeps
      // physical origin/destination and reverses them in StartLabel::point.
      if (reverse) q.flip_dir();
      auto result=pong_search(tt,nullptr,search,state,q,
          reverse?direction::kBackward:direction::kForward);
      std::vector<std::tuple<unsigned,long,long>> keys;
      for (auto const& j : *result.journeys_) {
        keys.emplace_back(j.transfers_+1U,
            std::chrono::duration_cast<std::chrono::seconds>(j.departure_time()-day).count(),
            std::chrono::duration_cast<std::chrono::seconds>(j.arrival_time()-day).count());
      }
      std::sort(keys.begin(),keys.end());
      for (auto const& [boardings,departure,arrival] : keys)
        std::cout << case_id << ' ' << reverse << ' ' << boardings << ' ' << departure << ' ' << arrival << '\n';
    }
  }
}
