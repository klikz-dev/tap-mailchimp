# The Singer Spec in php
This is a Singer tap that produces JSON-formatted data following the Singer spec.

This tap:

Pulls raw data from a source, which is used to:
* Create a table based on a schema returned
* Incrementally pull data based on the input state

# The Test Method
Uses credential data from the user to test if data can be retrieved from the source

# The Discover Method
Retrieves schema data from this taps source

Then outputs data to standard output, which is used to build a table for this tap

# The Tap Method
Retrieves a group of records from the taps source for a schema based on a sent in configuration

Then outputs data to standard output, which can be used to add records to that table
