# Scaffolding for interface-dispatched, patch-versioned Temporal workflows.
#
# Pattern (see workflows/processOrder for the reference implementation):
#   - One stably-named registered workflow per package: <Name>Workflow
#   - resolveFlowVersion() calls workflow.GetVersion once and dispatches the
#     whole run to the implementation matching that version.
#   - The CURRENT implementation always lives in the stable file
#     <name>Workflow.go (struct <name>Workflow). Cutting a new version COPIES
#     that file to a frozen <name>Workflow_vN.go snapshot, so git blame on the
#     living file stays clean and in-flight edits are never silently dropped.
#
# NAME is the bare camelCase action, e.g. `processOrder` (NOT `ProcessOrder`,
# NOT `processOrderWorkflow`).

set shell := ["bash", "-cu"]

workflows_dir := "workflows"

# List available recipes.
default:
    @just --list

alias scaffold := new

# Scaffold a brand-new versioned workflow at version 1.
new name:
    #!/usr/bin/env bash
    set -euo pipefail
    raw='{{name}}'
    camel="$(printf '%s' "${raw:0:1}" | tr '[:upper:]' '[:lower:]')${raw:1}"
    pascal="$(printf '%s' "${raw:0:1}" | tr '[:lower:]' '[:upper:]')${raw:1}"
    pkg="${camel}Workflow"
    dir="{{workflows_dir}}/${camel}"
    controller="${dir}/${camel}.go"
    stable="${dir}/${pkg}.go"
    iface="${camel}"
    pubfunc="${pascal}Workflow"
    inputt="${pascal}Input"
    resultt="${pascal}Result"
    curstruct="${pkg}"
    constname="${camel}VersionCurrent"
    changeid="workflow/${camel}"
    if [ -e "$dir" ]; then echo "error: $dir already exists" >&2; exit 1; fi
    mkdir -p "$dir"
    cat > "$controller" <<EOF
    package ${pkg}

    import (
    "fmt"

    "go.temporal.io/sdk/workflow"
    )

    type ${inputt} struct {
    }

    type ${resultt} struct {
    }

    const flowChangeID = "${changeid}"
    const MIN_VERSION = 1

    type ${iface} interface {
    run(ctx workflow.Context, input ${inputt}) (${resultt}, error)
    }

    func ${pubfunc}(ctx workflow.Context, input ${inputt}) (${resultt}, error) {
    return resolveFlowVersion(ctx).run(ctx, input)
    }

    func resolveFlowVersion(ctx workflow.Context) ${iface} {
    v := workflow.GetVersion(ctx, flowChangeID, MIN_VERSION, ${constname})

    versions := map[workflow.Version]${iface}{
    ${constname}: ${curstruct}{},
    }

    version, ok := versions[v]
    if !ok {
    panic(fmt.Sprintf("unsupported %s version %d", flowChangeID, v))
    }
    return version
    }
    EOF
    cat > "$stable" <<EOF
    package ${pkg}

    import (
    "go.temporal.io/sdk/workflow"
    )

    const ${constname} = 1

    type ${curstruct} struct{}

    func (${curstruct}) run(ctx workflow.Context, input ${inputt}) (${resultt}, error) {
    // TODO: implement the current version of ${pubfunc} here.
    return ${resultt}{}, nil
    }
    EOF
    gofmt -w "$controller" "$stable"
    # Auto-register the new workflow on the worker in main.go.
    main="main.go"
    module="$(go list -m 2>/dev/null || true)"
    registered="no"
    if [ -f "$main" ] && [ -n "$module" ] && grep -qF "w.RegisterActivity(" "$main"; then
    if ! grep -qF "/workflows/${camel}\"" "$main"; then
    perl -i -ne 'print "\t'"$pkg"' \"'"$module"'/workflows/'"$camel"'\"\n" if m{go\.temporal\.io/sdk/client} && !$i++; print;' "$main"
    fi
    if ! grep -qF "${pkg}.${pubfunc})" "$main"; then
    perl -i -ne 'print "\tw.RegisterWorkflow('"$pkg"'.'"$pubfunc"')\n" if /w\.RegisterActivity\(/ && !$j++; print;' "$main"
    fi
    gofmt -w "$main"
    registered="yes"
    fi
    go build ./...
    echo "Created ${dir}/ at version 1:"
    echo "  ${controller}  (interface, structs, resolver)"
    echo "  ${stable}  (current implementation)"
    if [ "$registered" = "yes" ]; then
    echo "Registered ${pkg}.${pubfunc} on the worker in ${main}."
    else
    echo "Could not auto-edit main.go — register manually:"
    echo "  import ${pkg} \"${module:-<module>}/workflows/${camel}\""
    echo "  w.RegisterWorkflow(${pkg}.${pubfunc})"
    fi

# Freeze the current version into a _vN.go snapshot and bump to the next version.
bump name:
    #!/usr/bin/env bash
    set -euo pipefail
    raw='{{name}}'
    camel="$(printf '%s' "${raw:0:1}" | tr '[:upper:]' '[:lower:]')${raw:1}"
    pkg="${camel}Workflow"
    dir="{{workflows_dir}}/${camel}"
    controller="${dir}/${camel}.go"
    stable="${dir}/${pkg}.go"
    curstruct="${pkg}"
    constname="${camel}VersionCurrent"
    if [ ! -f "$stable" ]; then echo "error: $stable not found (run 'just new ${camel}' first)" >&2; exit 1; fi
    if [ ! -f "$controller" ]; then echo "error: $controller not found" >&2; exit 1; fi
    n="$(grep -oE "const ${constname} = [0-9]+" "$stable" | grep -oE '[0-9]+$' || true)"
    if [ -z "$n" ]; then echo "error: could not find 'const ${constname} = <N>' in $stable" >&2; exit 1; fi
    next=$((n + 1))
    frozenstruct="${pkg}V${n}"
    frozen="${dir}/${pkg}_v${n}.go"
    if [ -e "$frozen" ]; then echo "error: $frozen already exists" >&2; exit 1; fi
    # 1. Freeze the current code: copy stable -> snapshot, rename the struct to
    #    <pkg>V<n>, and drop the version constant (it lives only in the stable file).
    cp "$stable" "$frozen"
    perl -i -ne 'if (/^package /){print;next} s/\b'"$curstruct"'\b/'"$frozenstruct"'/g; print unless /^const '"$constname"'\b/;' "$frozen"
    # 2. Bump the version constant in the stable (living) file.
    perl -i -pe 's/(const '"$constname"'\s*=\s*)[0-9]+/${1}'"$next"'/' "$stable"
    # 3. Register the frozen version in the resolver map.
    perl -i -ne 'print "\t\t'"$n"': '"$frozenstruct"'{},\n" if /^\s*'"$constname"':/; print;' "$controller"
    gofmt -w "$frozen" "$stable" "$controller"
    go build ./...
    echo "Bumped ${camel}: v${n} -> v${next}"
    echo "  froze v${n} into ${frozen} (struct ${frozenstruct}, registered in resolver)"
    echo "  ${stable} is now version ${next} — edit it in place for the new behavior."

# Retire old versions: delete their snapshots, drop their resolver entries, and
# raise MIN_VERSION. MIN_VERSION is a floor, so retirement is always contiguous
# from the oldest live version. With no VERSION, retires just the oldest; with a
# VERSION, retires everything from the oldest up THROUGH that version.
# Only safe once no executions remain on the retired version(s).
retire name version='':
    #!/usr/bin/env bash
    set -euo pipefail
    raw='{{name}}'
    version='{{version}}'
    camel="$(printf '%s' "${raw:0:1}" | tr '[:upper:]' '[:lower:]')${raw:1}"
    pkg="${camel}Workflow"
    dir="{{workflows_dir}}/${camel}"
    controller="${dir}/${camel}.go"
    stable="${dir}/${pkg}.go"
    constname="${camel}VersionCurrent"
    if [ ! -f "$controller" ] || [ ! -f "$stable" ]; then echo "error: ${dir} not found (run 'just new ${camel}' first)" >&2; exit 1; fi
    m="$(grep -oE 'const MIN_VERSION = [0-9]+' "$controller" | grep -oE '[0-9]+$' || true)"
    cur="$(grep -oE "const ${constname} = [0-9]+" "$stable" | grep -oE '[0-9]+$' || true)"
    if [ -z "$m" ] || [ -z "$cur" ]; then echo "error: could not read MIN_VERSION / ${constname}" >&2; exit 1; fi
    if [ "$m" -ge "$cur" ]; then echo "error: nothing to retire — lowest live version (${m}) is the current version (${cur})" >&2; exit 1; fi
    if [ -z "$version" ]; then target="$m"; else target="$version"; fi
    if ! [[ "$target" =~ ^[0-9]+$ ]]; then echo "error: VERSION must be an integer, got '${target}'" >&2; exit 1; fi
    if [ "$target" -lt "$m" ]; then echo "error: v${target} is already retired (MIN_VERSION is ${m})" >&2; exit 1; fi
    if [ "$target" -ge "$cur" ]; then echo "error: cannot retire the current version (${cur}); retire only frozen versions (< ${cur})" >&2; exit 1; fi
    # Retire each version in [m .. target]: remove its snapshot and resolver entry.
    for (( v=m; v<=target; v++ )); do
    frozenstruct="${pkg}V${v}"
    frozen="${dir}/${pkg}_v${v}.go"
    if [ -f "$frozen" ]; then rm "$frozen"; else echo "warning: ${frozen} not found, skipping file removal" >&2; fi
    perl -i -ne 'print unless /^\s*'"$v"':\s*'"$frozenstruct"'\{\}/;' "$controller"
    done
    next_min=$((target + 1))
    # Raise MIN_VERSION so GetVersion no longer supports the retired version(s).
    perl -i -pe 's/(const MIN_VERSION\s*=\s*)[0-9]+/${1}'"$next_min"'/' "$controller"
    gofmt -w "$controller"
    go build ./...
    if [ "$target" -eq "$m" ]; then range="v${m}"; else range="v${m}..v${target}"; fi
    echo "Retired ${camel} ${range}: removed snapshot(s), dropped resolver entries, MIN_VERSION ${m} -> ${next_min}."
    echo "WARNING: only safe once no executions remain on ${range} — GetVersion will now reject histories pinned below v${next_min}."

# Show the version constant and frozen snapshots for a workflow.
versions name:
    #!/usr/bin/env bash
    set -euo pipefail
    raw='{{name}}'
    camel="$(printf '%s' "${raw:0:1}" | tr '[:upper:]' '[:lower:]')${raw:1}"
    pkg="${camel}Workflow"
    dir="{{workflows_dir}}/${camel}"
    grep -hoE "const ${camel}VersionCurrent = [0-9]+" "${dir}/${pkg}.go" || true
    ls -1 "${dir}/${pkg}"_v*.go 2>/dev/null || echo "(no frozen versions yet)"
