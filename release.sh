#!/bin/bash

# 1. read latest tag (if none, start from v0.0.1)
CURRENT_VERSION=$(git describe --tags --abbrev=0 2>/dev/null)

if [ -z "$CURRENT_VERSION" ]; then
    CURRENT_VERSION="v0.0.1"
    echo "no tags found. starting at $CURRENT_VERSION"
else
    echo "current version: $CURRENT_VERSION"
fi

# strip optional leading v to work with numeric parts only
VERSION_WITHOUT_V="${CURRENT_VERSION#v}"

# split semver into major, minor, patch (e.g. 0 0 9)
IFS='.' read -r MAJOR MINOR PATCH <<< "$VERSION_WITHOUT_V"

# 2. bump patch
PATCH=$((PATCH + 1))

# carry when patch reaches 10: reset patch, bump minor
if [ "$PATCH" -ge 10 ]; then
    PATCH=0
    MINOR=$((MINOR + 1))
fi

# assemble new version
NEW_VERSION="v${MAJOR}.${MINOR}.${PATCH}"
echo "new version: $NEW_VERSION"

# 3. git release steps
echo "staging files..."
git add .

# empty commit if nothing staged
if git diff --staged --quiet; then
    echo "no staged changes; creating empty commit for tag."
    git commit --allow-empty -m "Release $NEW_VERSION"
else
    git commit -m "Release $NEW_VERSION"
fi

echo "creating tag $NEW_VERSION..."
git tag "$NEW_VERSION"

echo "pushing branch main to origin..."
git push origin main

echo "pushing tag to origin..."
git push origin "$NEW_VERSION"

echo "done. published version $NEW_VERSION."