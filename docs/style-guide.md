## Class Structure

* Create a `Static` inner class for all `static` methods.

## Naming

* Prefix with `get` and `set` when exposing an internal property.
* Prefix with `get`, `put`, and `remove` when exposing a `Map`-like property.
* Prefix with `get`, `add`, and `remove` when exposing a `Collection`-like property.
* Avoid `get` otherwise; `find` is a good alternative.
