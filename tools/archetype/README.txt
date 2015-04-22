Archetypes
==========

Maven archetypes are project templates used to generate new Maven projects.
The archetype(s) here is to generate a Lily project to start your own
development from.


Basic Lily Project
------------------

To generate a project use (adjust version to current one):

mvn archetype:generate \
  -DarchetypeGroupId=org.lilyproject \
  -DarchetypeArtifactId=lily-archetype-basic \
  -DarchetypeVersion=2.6.1

MapReduce Job
-------------

mvn archetype:generate \
  -DarchetypeGroupId=org.lilyproject \
  -DarchetypeArtifactId=lily-archetype-mapreduce \
  -DarchetypeVersion=2.6.1

Repository Decorator
--------------------

mvn archetype:generate \
  -DarchetypeGroupId=org.lilyproject \
  -DarchetypeArtifactId=lily-archetype-lily-server-plugin \
  -DarchetypeVersion=2.6.1
