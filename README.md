Example showing using versioning via patching, but keeping separate interface implementations to resolve version.

This becomes a form of workflow versioning, without using different registered names for the workflows.

The structure allows for automating scaffolding.

The just file provides the following commands:

`just bump {name}` <- takes the current version and creates the frozen version file, and bump the current version by 1. Updates the resolver as well. Eg: `just bump processOrder`

`just new {name}` <- scaffolds a new workflow with an immediate v1 marker and resolver

`just retire {name}` <- by default, retires the oldest version and removes it from the resolver

`just retire {name} {version}` <- retires the specified version (and all older versions)
