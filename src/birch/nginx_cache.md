# place this in sites-enabled
```
 proxy_cache_path    /var/cache/nginx/backend_cache
                        levels=1:2
                        max_size=64g
                        inactive=24h
                        use_temp_path=off
                        keys_zone=catenary:10m;

server {
  location / {
  proxy_pass http://localhost:17419;

  proxy_cache catenary;
}

listen 17420;
listen [::]:17420;
}
```

