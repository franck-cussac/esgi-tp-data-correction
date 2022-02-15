# TP ESGI IABD 5è année

## Comment construire un jar

Il vous faudra maven 3.3.9 sinon ça ne fonctionnera pas

```bash
mvn package
```

Si vous n'avez pas maven 3.3.9, vous pouvez installer Docker et compiler comme ça :

```bash
DOCKER_BUILDKIT=1 docker build . -o target/<monjar>.jar
```

Votre jar apparaitra dans le dossier `target/`

