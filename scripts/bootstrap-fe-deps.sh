#!/usr/bin/env bash
set -euo pipefail

root="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
version="$(sed -n 's/^[[:space:]]*<doris\.version>\([^<]*\)<\/doris\.version>[[:space:]]*$/\1/p' "$root/pom.xml")"

fail() {
    printf '%s\n' "$*" >&2
    exit 1
}

if [[ ! ${DORIS_FE_LIB+x} && -f "$root/.env" ]]; then
    DORIS_FE_LIB="$(sed -n 's/^[[:space:]]*DORIS_FE_LIB[[:space:]]*=[[:space:]]*//p' "$root/.env" \
        | tail -n 1 | sed 's/[[:space:]]*$//; s/^"\(.*\)"$/\1/; s/^'\''\(.*\)'\''$/\1/')"
fi
[[ ${DORIS_FE_LIB:-} = /* && -d "$DORIS_FE_LIB" ]] \
    || fail "Set DORIS_FE_LIB to the absolute path of the Doris $version release fe/lib directory."
lib="$(cd -- "$DORIS_FE_LIB" && pwd)"
jdk_bin=""
if [[ -n ${JAVA_HOME:-} ]]; then
    jdk_bin="${JAVA_HOME%/}/bin/"
fi

core="$lib/doris-fe.jar"
common=("$lib"/fe-common-*.jar)
spi=("$lib"/fe-extension-spi-*.jar)
[[ ${#common[@]} -eq 1 && ${#spi[@]} -eq 1 && -f "$core" && -f "${common[0]}" && -f "${spi[0]}" ]] \
    || fail "$lib must contain doris-fe.jar and exactly one fe-common and fe-extension-spi JAR."

IFS=. read -r major minor patch <<< "$version"
build="$("${jdk_bin}javap" -constants -classpath "$core" org.apache.doris.common.Version)" \
    || fail "Could not read Doris release version from $core."
for entry in "MAJOR = $major;" "MINOR = $minor;" "PATCH = $patch;" "HOTFIX = 0;"; do
    [[ "$build" == *"DORIS_BUILD_VERSION_$entry"* ]] \
        || fail "$core must be from the Doris $version binary release."
done

require_classes() {
    local jar="$1" entries class
    shift
    entries="$("${jdk_bin}jar" tf "$jar")" || fail "Could not read $jar."
    for class in "$@"; do
        grep -Fqx "$class.class" <<< "$entries" || fail "$jar is missing $class.class."
    done
}

require_classes "$core" \
    org/apache/doris/nereids/lineage/{LineageInfo,LineageContext,LineagePlugin,LineagePluginFactory} \
    org/apache/doris/catalog/{Column,TableIf} org/apache/doris/datasource/ExternalTable \
    org/apache/doris/nereids/trees/expressions/{Expression,Slot,SlotReference}
require_classes "${common[0]}" org/apache/doris/catalog/Type
require_classes "${spi[0]}" org/apache/doris/extension/spi/{Plugin,PluginContext,PluginFactory}

temp="$(mktemp -d)"
trap 'rm -rf -- "$temp"' EXIT

install_jar() {
    local artifact="$1" jar="$2"
    shift 2
    local pom="$temp/$artifact.pom"
    cat > "$pom" <<POM
<project xmlns="http://maven.apache.org/POM/4.0.0">
  <modelVersion>4.0.0</modelVersion>
  <groupId>org.apache.doris</groupId>
  <artifactId>$artifact</artifactId>
  <version>$version</version>
</project>
POM
    mvn -B "$@" org.apache.maven.plugins:maven-install-plugin:3.1.2:install-file "-Dfile=$jar" "-DpomFile=$pom"
}

install_jar fe-core "$core" "$@"
install_jar fe-common "${common[0]}" "$@"
install_jar fe-extension-spi "${spi[0]}" "$@"
