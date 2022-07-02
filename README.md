# astro_python_neo4j

This is a bunch of trash code intended to import various astronomical libraries into a neo4j graph database.

It's not the prettiest code, but it works.

# app.properties

If you are crazy enough to use this code, you need to create an app.properties file that looks something like this:

```
[Database]
uri = (bolt uri)
user = neo4j
password = (password)
```

# order to run the scripts

Run the scripts in the following order:

- init.py
- catalogs.py
- spectral_types.py
- (individual catalogs)
