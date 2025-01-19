### **Rapport détaillé du TD : Hadoop et le traitement de données distribuées**

---
docker cp target/hadoop-first-code-1.0-SNAPSHOT.jar namenode:/tmp/

### **1. Objectifs du TD**

L’objectif principal de ce TD était d’explorer les capacités d’Hadoop pour traiter des données distribuées à l’aide de MapReduce. Les étapes comprenaient :
- Installation et déploiement d’un cluster Hadoop à l’aide de Docker.
- Exécution de programmes MapReduce simples.
- Lecture et manipulation de fichiers HDFS.
- Développement de programmes MapReduce avancés impliquant des jointures et des traitements multi-tâches.
- Analyse de données réelles à partir d’un jeu de données complexe (GroupLens).

---

### **2. Déploiement et configuration du cluster Hadoop**

#### **2.1. Docker Compose**
- **Modification clé** : Le réseau Docker a été renommé en `hadoop` (au lieu de `hadoop_default`) pour éviter les problèmes liés aux underscores dans les noms de domaine.
- **Nouveaux services ajoutés** :
    - **ResourceManager** : Gère les ressources et planifie les tâches.
    - **NodeManager** : Exécute les conteneurs YARN pour traiter les tâches.
    - **HistoryServer** : Stocke l’historique des tâches MapReduce.

#### **2.2. Commandes de base**
- Supprimer les conteneurs et le réseau existants :
  ```bash
  docker-compose down -v
  ```
- Déployer le cluster Hadoop :
  ```bash
  docker-compose up -d
  ```

---

### **3. Tests du cluster Hadoop**

#### **3.1. Exécution d’un exemple MapReduce**
- Commande utilisée pour exécuter l'exemple **Pi** :
  ```bash
  hadoop jar /path/to/hadoop-mapreduce-examples-3.2.1.jar pi 100 10
  ```
- **Résultats observés** :
    - Création de fichiers dans HDFS.
    - Affichage des détails de l’application sur l’interface web du ResourceManager.

#### **3.2. Analyse des paramètres**
- **Paramètre 1 (100)** : Nombre de tâches Map.
- **Paramètre 2 (10)** : Nombre d’échantillons générés par chaque tâche.

---

### **4. Développement de programmes Hadoop**

#### **4.1. Création d’un projet Maven**
- Commande pour générer un projet Maven :
  ```bash
  mvn archetype:generate -DgroupId=org.hadoop.examples -DartifactId=hadoop-first-code -DarchetypeArtifactId=maven-archetype-quickstart -DinteractiveMode=false
  ```
- Ajout des dépendances suivantes au fichier `pom.xml` :
  ```xml
  <dependency>
      <groupId>org.apache.hadoop</groupId>
      <artifactId>hadoop-common</artifactId>
      <version>3.3.1</version>
  </dependency>
  <dependency>
      <groupId>org.apache.hadoop</groupId>
      <artifactId>hadoop-mapreduce-client-core</artifactId>
      <version>3.3.1</version>
  </dependency>
  ```

---

### **5. Lecture de fichiers HDFS avec Mapper et Reducer**

#### **Objectif :**
- Lire des fichiers dans HDFS.
- Mapper : Envoyer `(clé, ligne)`.
- Reducer : Écrire `(clé, [valeurs])` sur le disque.

#### **Exemple de commande** :
- **Copier un fichier dans HDFS** :
  ```bash
  hdfs dfs -mkdir /input
  hdfs dfs -put /local/path/to/file.txt /input/
  ```
- **Exécuter le programme MapReduce** :
  ```bash
  hadoop jar /tmp/hadoop-first-code-1.0-SNAPSHOT.jar org.hadoop.examples.driver.SimpleJobDriver /input /output
  ```

#### **Résultats** :
- Les clés représentent les décalages des lignes dans le fichier.
- Les valeurs contiennent le contenu des lignes.

---

### **6. Traitement d’un jeu de données complexe**

#### **6.1. Jeu de données utilisé : GroupLens (ml-25m.zip)**
- Fichiers inclus :
    - `movies.csv` : Informations sur les films.
    - `ratings.csv` : Notes attribuées par les utilisateurs.
    - `tags.csv` : Tags associés aux films.
    - `genome-tags.csv` et `genome-scores.csv` : Données de pertinence des tags.

#### **6.2. Programmes développés :**

1. **Nombre de films par genre :**
    - Mapper : Émet `(genre, 1)` pour chaque film.
    - Reducer : Somme les valeurs pour chaque genre.
    - Résultat : Une liste des genres avec le nombre de films associés.

2. **Liste des films pour un genre donné :**
    - Mapper : Filtre les films appartenant au genre spécifié.
    - Reducer : Agrège les titres des films.

3. **Score moyen par `userID` et `movieID` :**
    - Mapper : Émet `(userID, rating)` ou `(movieID, rating)`.
    - Reducer : Calcule la moyenne.

4. **Film le mieux noté par utilisateur :**
    - Mapper : Émet `(userID, movieID:rating)`.
    - Reducer : Identifie le film avec le score maximal pour chaque utilisateur.

---

### **7. Traitements avancés :**

#### **7.1. Nom du film le mieux noté par utilisateur (jointure)**
- **Approche** :
    - Mapper 1 (Ratings) : Émet `(movieID, userID:rating)`.
    - Mapper 2 (Movies) : Émet `(movieID, MOVIE:title)`.
    - Reducer : Combine les données pour associer les titres aux scores.

#### **7.2. Film le mieux noté par genre**
- **Approche** :
    - Mapper : Associe chaque film à ses genres.
    - Reducer : Identifie le film avec le score maximum pour chaque genre.

#### **7.3. Tag le mieux noté**
- **Approche** :
    - Mapper : Associe chaque tag à sa pertinence (`genome-scores.csv`).
    - Reducer : Identifie le tag avec la pertinence maximale.

---

### **8. Problèmes rencontrés et solutions**

#### **8.1. Erreurs liées au format des données**
- **Problème :** Certaines lignes de données contenaient des en-têtes ou étaient mal formées.
- **Solution :** Ajouter des validations dans les Mappers et Reducers pour ignorer les lignes incorrectes.

#### **8.2. Problèmes de jointure**
- **Problème :** Les données des Mappers ne correspondaient pas toujours.
- **Solution :** Utiliser un préfixe (`MOVIE:`) pour distinguer les sources dans le Reducer.

#### **8.3. Échecs de tâches MapReduce**
- **Problème :** Certaines tâches échouaient en raison d’exceptions (par ex. `ArrayIndexOutOfBoundsException`).
- **Solution :** Ajouter des validations supplémentaires pour vérifier la présence de séparateurs dans les données.

---

### **9. Résultats obtenus**

#### **Exemples de sorties :**
- **Nombre de films par genre** :
  ```
  Adventure: 1200
  Comedy: 3400
  Drama: 4500
  ```
- **Films pour le genre "Comedy"** :
  ```
  Comedy -> [MovieA, MovieB, MovieC]
  ```
- **Score moyen par utilisateur** :
  ```
  UserID: 1 -> Average Rating: 4.2
  UserID: 2 -> Average Rating: 3.8
  ```

---

### **10. Réflexions et apprentissages**

- **Compréhension renforcée** :
    - Fonctionnement des Mappers, Reducers et du Driver Hadoop.
    - Importance de la validation des données.
    - Gestion des jointures et des traitements multi-tâches.

- **Limites identifiées** :
    - Complexité accrue pour les jointures impliquant de grands ensembles de données.
    - Difficulté de débogage dans les environnements distribués.

- **Recommandations pour la suite :**
    - Étudier les alternatives modernes comme Apache Spark pour des traitements plus rapides et flexibles.
    - Explorer d'autres cas d'utilisation avec des jeux de données encore plus complexes.

---

### **11. Commandes principales utilisées**

#### **Copie des fichiers dans HDFS :**
```bash
hdfs dfs -mkdir /input
hdfs dfs -put /local/path/to/file.csv /input/
```

#### **Exécution d’un job Hadoop :**
```bash
hadoop jar /tmp/hadoop-first-code-1.0-SNAPSHOT.jar org.hadoop.examples.driver.<JobName> /input /output
```

#### **Consultation des résultats :**
```bash
hdfs dfs -cat /output/part-r-00000
```

---

**Ce rapport résume les étapes, les concepts et les difficultés rencontrées durant ce TD. Hadoop reste un outil puissant pour traiter de grandes quantités de données, mais il exige une rigueur particulière pour assurer le bon fonctionnement des programmes MapReduce.**