# docs/

Suggested virtual environment: [pipenv](https://pipenv.pypa.io/en/latest/)

Install dependencies: 

```
pipenv install
```

Autobuild: 

```
pipenv run sphinx-autobuild source build/html
```

Build static files: 

```
pipenv run make html
```

## Templates

The files in `source/_templates/` override the default [Alabaster theme templates](https://github.com/bitprophet/alabaster/tree/master/alabaster).

## Headers

Be aware that header level hierachy is enforced by something in Sphinx. H1 > H2 > H3, and so on. If you try to use an H3 after an H1, Sphinx will output an H2 instead.
