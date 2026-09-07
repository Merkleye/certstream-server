"use strict";

// Appends a "Container Image" section to the notes semantic-release already
// generated from commits. semantic-release concatenates every plugin's
// generateNotes output in plugins-array order, so this just needs to run
// after @semantic-release/release-notes-generator — the result lands in both
// CHANGELOG.md (via @semantic-release/changelog) and the GitHub Release body
// (via @semantic-release/github), since both consume nextRelease.notes.
//
// Tag-based, not digest-based: the per-platform image digest isn't known
// until scripts/release-image.sh actually builds and pushes, which happens
// later in the `prepare` lifecycle step than generateNotes.
//
// Mirrors merkleye/certspotter and merkleye/dnstwist's
// scripts/semantic-release-image-links.cjs, narrowed to the one image this
// repo owns.

const REGISTRY = "ghcr.io/merkleye";
const REPO = "merkleye/certstream-server";
const IMAGE = "certstream-server";

module.exports = {
  generateNotes: async (_pluginConfig, context) => {
    const version = context.nextRelease.version;
    const lines = [
      "",
      "## Container Image",
      "",
      `- \`docker pull ${REGISTRY}/${IMAGE}:v${version}\` — [package page](https://github.com/${REPO}/pkgs/container/${IMAGE})`,
    ];
    return lines.join("\n");
  },
};
