## EntityLocker library

EntityLocker is a reusable utility class that provides synchronization mechanism similar to row-level DB locking.

The class is supposed to be used by the components that are responsible for managing storage and caching of different type of entities in the application. EntityLocker itself does not deal with the entities, only with the IDs (primary keys) of the entities.

See full task description in `doc/EntityLocker.txt`

### Usage

Prerequisites: JDK 17 (tested on OpenJDK 17.0.1).

The library can be build with `./gradlew clean build` command.

See tests for the class usage examples.
