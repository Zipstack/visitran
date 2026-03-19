# Events Module

The Events Module of Visitran captures all the internal Events to a Loggable Interface. We are using Betterproto for Compiling
the protobuf message into .py file structure.

## Using the Events Module

The event module provides types that represent what is happening in Visitran in `events.types`. These types are intended to
represent an exhaustive list of all things happening within Visitran that will need to be logged, streamed, or printed. To fire
an event, `events.functions::fire_event` is the entry point to the module from everywhere in Visitran.

## Logging

When events are processed via `fire_event`, nearly everything is logged. Whether or not the user has enabled the debug flag, all
debug messages are still logged to the file.

## Adding a New Event

* Add a new message in types.proto with an EventInfo field first

* Run ``` poetry add --group dev protobuf ``` to add protobuf package in the list of development dependencies.
* Run the protoc compiler to update proto_types.py:  ```protoc --python_betterproto_out . types.proto```
* Add a wrapping class in /visitran/event/types.py with a Level superclass and the superclass from proto_types.py,
plus code and message methods

Note that no attributes can exist in these event classes except for fields defined in the protobuf definitions, because the
betterproto metaclass will throw an error. Betterproto provides a to_dict() method to convert the generated classes to a
dictionary and from that to json. However some attributes will successfully convert to dictionaries but not to serialized
protobufs, so we need to test both output formats.

## Required for Every Event

* a method `code`, that's unique across events
* assign a log level by using the Level mixin: `DebugLevel`, `InfoLevel`, `WarnLevel`, or `ErrorLevel`
* a message()

## Compiling types.proto

After adding a new message in types.proto, in the core/visitran/events directory:

```protoc --python_betterproto_out . types.proto```
