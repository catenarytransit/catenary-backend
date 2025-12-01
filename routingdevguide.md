### Install Postgres

You can do this either in docker or without docker. 

### setup postgres

Install the cargo diesel cli.

```bash
diesel database setup
diesel migration run
```

### Run the run local graph gen docker container

```bash
./run_local_graph_gen.sh
```

To run it without regenerating the OSM data

```bash
./run_local_graph_gen.sh --gentian-only
```


### Start the query server

```bash
./run_local_edelweiss.sh
```

At the same time, send it queries

```bash
./run_queries_local.sh
```
