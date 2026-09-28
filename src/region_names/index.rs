use super::{
    CountriesFile, GeoId, GeoNode, GeographyConfig, LocaleId, RegionNamesError, RegionsFile,
    SUPPORTED_SCHEMA_VERSION,
};
use std::collections::{HashMap, HashSet};

#[derive(Debug, Clone, Hash, PartialEq, Eq)]
struct SlugLookupKey {
    parent: Option<GeoId>,
    locale: LocaleId,
    slug: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct SlugLookupValue {
    region_id: GeoId,
    canonical: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResolvedGeoSlug {
    pub region_id: GeoId,

    /// False when at least one path component was an old alias and the caller
    /// should normally redirect to the current canonical URL.
    pub canonical: bool,
}

#[derive(Debug)]
pub struct GeographyIndex {
    config: GeographyConfig,
    nodes: HashMap<GeoId, GeoNode>,
    children: HashMap<GeoId, Vec<GeoId>>,
    roots: Vec<GeoId>,
    slug_lookup: HashMap<SlugLookupKey, SlugLookupValue>,
}

impl GeographyIndex {
    pub fn from_files(
        config: GeographyConfig,
        countries: CountriesFile,
        region_files: Vec<RegionsFile>,
    ) -> Result<Self, RegionNamesError> {
        validate_schema_version("config.toml", config.schema_version)?;
        validate_schema_version("countries.toml", countries.schema_version)?;

        let mut nodes = HashMap::new();

        for definition in countries.countries {
            let id = definition.id.clone();
            if nodes
                .insert(id.clone(), GeoNode::from_definition(definition, None))
                .is_some()
            {
                return Err(RegionNamesError::DuplicateGeoId(id));
            }
        }

        for regions_file in region_files {
            validate_schema_version(
                &format!("regions/{}.toml", regions_file.parent),
                regions_file.schema_version,
            )?;

            let parent = regions_file.parent;
            for definition in regions_file.regions {
                let id = definition.id.clone();
                if id == parent {
                    return Err(RegionNamesError::SelfParent(id));
                }

                if nodes
                    .insert(
                        id.clone(),
                        GeoNode::from_definition(definition, Some(parent.clone())),
                    )
                    .is_some()
                {
                    return Err(RegionNamesError::DuplicateGeoId(id));
                }
            }
        }

        for node in nodes.values() {
            if let Some(parent) = &node.parent {
                if !nodes.contains_key(parent) {
                    return Err(RegionNamesError::UnknownParent {
                        region: node.id.clone(),
                        parent: parent.clone(),
                    });
                }
            }
        }

        let ids = nodes.keys().cloned().collect::<Vec<_>>();
        let mut depths = HashMap::new();
        for id in &ids {
            let mut visiting = HashSet::new();
            calculate_depth(id, &nodes, &mut depths, &mut visiting)?;
        }

        for (id, depth) in depths {
            if let Some(node) = nodes.get_mut(&id) {
                node.depth = depth;
            }
        }

        validate_nodes(&config, &nodes)?;

        let mut roots = Vec::new();
        let mut children: HashMap<GeoId, Vec<GeoId>> = HashMap::new();
        for node in nodes.values() {
            match &node.parent {
                Some(parent) => children
                    .entry(parent.clone())
                    .or_default()
                    .push(node.id.clone()),
                None => roots.push(node.id.clone()),
            }
        }

        roots.sort();
        for child_ids in children.values_mut() {
            child_ids.sort();
        }

        let slug_lookup = build_slug_lookup(&nodes)?;

        Ok(Self {
            config,
            nodes,
            children,
            roots,
            slug_lookup,
        })
    }

    pub fn config(&self) -> &GeographyConfig {
        &self.config
    }

    pub fn node(&self, id: &str) -> Option<&GeoNode> {
        self.nodes.get(id)
    }

    pub fn contains(&self, id: &str) -> bool {
        self.nodes.contains_key(id)
    }

    pub fn roots(&self) -> impl Iterator<Item = &GeoNode> {
        self.roots.iter().filter_map(|id| self.nodes.get(id))
    }

    pub fn children_of(&self, id: &str) -> Vec<&GeoNode> {
        self.children
            .get(id)
            .into_iter()
            .flatten()
            .filter_map(|child_id| self.nodes.get(child_id))
            .collect()
    }

    pub fn parent_of(&self, id: &str) -> Option<&GeoNode> {
        let parent = self.nodes.get(id)?.parent.as_ref()?;
        self.nodes.get(parent)
    }

    /// Returns the requested localized name, then falls back to the configured
    /// fallback locale. Agency, route, and stop names should not use this.
    pub fn name<'a>(&'a self, id: &str, locale: &str) -> Option<&'a str> {
        let node = self.nodes.get(id)?;
        node.names
            .get(locale)
            .or_else(|| node.names.get(&self.config.fallback_locale))
            .map(String::as_str)
    }

    /// Returns only the exact locale's slug. URL slugs intentionally do not
    /// fall back to another locale.
    pub fn slug<'a>(&'a self, id: &str, locale: &str) -> Option<&'a str> {
        self.nodes.get(id)?.slugs.get(locale).map(String::as_str)
    }

    /// Page-locale name first, followed by all other official local-language
    /// names with duplicate spellings removed.
    pub fn display_names(&self, id: &str, locale: &str) -> Option<Vec<String>> {
        let node = self.nodes.get(id)?;
        let mut output = Vec::new();

        if let Some(primary) = self.name(id, locale) {
            push_unique(&mut output, primary);
        }

        for official_language in &node.official_languages {
            if let Some(name) = node.names.get(official_language) {
                push_unique(&mut output, name);
            }
        }

        Some(output)
    }

    /// Ancestors from root to immediate parent.
    pub fn ancestor_ids(&self, id: &str) -> Result<Vec<GeoId>, RegionNamesError> {
        let node = self
            .nodes
            .get(id)
            .ok_or_else(|| RegionNamesError::UnknownRegion(id.to_string()))?;

        let mut ancestors = Vec::new();
        let mut current = node.parent.as_deref();
        while let Some(parent_id) = current {
            let parent =
                self.nodes
                    .get(parent_id)
                    .ok_or_else(|| RegionNamesError::UnknownParent {
                        region: id.to_string(),
                        parent: parent_id.to_string(),
                    })?;
            ancestors.push(parent.id.clone());
            current = parent.parent.as_deref();
        }

        ancestors.reverse();
        Ok(ancestors)
    }

    /// Root -> ... -> entity, including the entity itself.
    pub fn lineage_ids(&self, id: &str) -> Result<Vec<GeoId>, RegionNamesError> {
        let mut lineage = self.ancestor_ids(id)?;
        lineage.push(id.to_string());
        Ok(lineage)
    }

    /// Generates a localized slug path from stable IDs.
    ///
    /// `localized_path("DE-BY", "de")` -> `["deutschland", "bayern"]`.
    pub fn localized_path(&self, id: &str, locale: &str) -> Result<Vec<String>, RegionNamesError> {
        self.lineage_ids(id)?
            .into_iter()
            .map(|region_id| {
                self.slug(&region_id, locale)
                    .map(str::to_string)
                    .ok_or_else(|| RegionNamesError::MissingSlugForPath {
                        region: region_id,
                        locale: locale.to_string(),
                    })
            })
            .collect()
    }

    /// Resolves a child slug relative to its stable parent ID.
    pub fn resolve_child(
        &self,
        parent: Option<&str>,
        locale: &str,
        slug: &str,
    ) -> Option<ResolvedGeoSlug> {
        let key = SlugLookupKey {
            parent: parent.map(str::to_string),
            locale: locale.to_string(),
            slug: slug.to_string(),
        };

        self.slug_lookup.get(&key).map(|value| ResolvedGeoSlug {
            region_id: value.region_id.clone(),
            canonical: value.canonical,
        })
    }

    /// Resolves a complete localized path back to one stable GeoId.
    pub fn resolve_path(&self, locale: &str, slugs: &[&str]) -> Option<ResolvedGeoSlug> {
        let mut parent: Option<GeoId> = None;
        let mut canonical = true;

        for slug in slugs {
            let resolved = self.resolve_child(parent.as_deref(), locale, slug)?;
            canonical &= resolved.canonical;
            parent = Some(resolved.region_id);
        }

        parent.map(|region_id| ResolvedGeoSlug {
            region_id,
            canonical,
        })
    }

    /// Returns the member of an entity's lineage at the requested depth.
    /// Depth 0 is the root/country.
    pub fn ancestor_at_depth(
        &self,
        id: &str,
        depth: usize,
    ) -> Result<Option<GeoId>, RegionNamesError> {
        for region_id in self.lineage_ids(id)? {
            if self
                .nodes
                .get(&region_id)
                .is_some_and(|node| node.depth == depth)
            {
                return Ok(Some(region_id));
            }
        }
        Ok(None)
    }
}

pub(crate) fn validate_schema_version(
    location: &str,
    schema_version: u32,
) -> Result<(), RegionNamesError> {
    if schema_version != SUPPORTED_SCHEMA_VERSION {
        return Err(RegionNamesError::UnsupportedSchemaVersion {
            location: location.to_string(),
            expected: SUPPORTED_SCHEMA_VERSION,
            actual: schema_version,
        });
    }
    Ok(())
}

fn calculate_depth(
    id: &str,
    nodes: &HashMap<GeoId, GeoNode>,
    depths: &mut HashMap<GeoId, usize>,
    visiting: &mut HashSet<GeoId>,
) -> Result<usize, RegionNamesError> {
    if let Some(depth) = depths.get(id) {
        return Ok(*depth);
    }

    if !visiting.insert(id.to_string()) {
        return Err(RegionNamesError::Cycle(id.to_string()));
    }

    let node = nodes
        .get(id)
        .ok_or_else(|| RegionNamesError::UnknownRegion(id.to_string()))?;

    let depth = match &node.parent {
        None => 0,
        Some(parent) => calculate_depth(parent, nodes, depths, visiting)? + 1,
    };

    visiting.remove(id);
    depths.insert(id.to_string(), depth);
    Ok(depth)
}

fn validate_nodes(
    config: &GeographyConfig,
    nodes: &HashMap<GeoId, GeoNode>,
) -> Result<(), RegionNamesError> {
    for node in nodes.values() {
        if !node.names.contains_key(&config.fallback_locale) {
            return Err(RegionNamesError::MissingFallbackName {
                region: node.id.clone(),
                locale: config.fallback_locale.clone(),
            });
        }

        for language in &node.official_languages {
            if !node.names.contains_key(language) {
                return Err(RegionNamesError::MissingOfficialName {
                    region: node.id.clone(),
                    locale: language.clone(),
                });
            }
        }

        if config.require_public_locale_slugs {
            for locale in &config.public_locales {
                if !node.slugs.contains_key(locale) {
                    return Err(RegionNamesError::MissingPublicSlug {
                        region: node.id.clone(),
                        locale: locale.clone(),
                    });
                }
            }
        }

        for (locale, slug) in &node.slugs {
            validate_slug(config, &node.id, locale, slug)?;
        }
        for (locale, aliases) in &node.slug_aliases {
            for alias in aliases {
                validate_slug(config, &node.id, locale, alias)?;
            }
        }
    }
    Ok(())
}

fn validate_slug(
    config: &GeographyConfig,
    region: &str,
    locale: &str,
    slug: &str,
) -> Result<(), RegionNamesError> {
    if slug.is_empty() {
        return Err(RegionNamesError::EmptySlug {
            region: region.to_string(),
            locale: locale.to_string(),
        });
    }
    if slug.contains('/') {
        return Err(RegionNamesError::InvalidSlug {
            region: region.to_string(),
            locale: locale.to_string(),
            slug: slug.to_string(),
            reason: "slugs may not contain '/'".to_string(),
        });
    }
    if !config.allow_unicode_slugs && !slug.is_ascii() {
        return Err(RegionNamesError::InvalidSlug {
            region: region.to_string(),
            locale: locale.to_string(),
            slug: slug.to_string(),
            reason: "Unicode slugs are disabled".to_string(),
        });
    }
    Ok(())
}

fn build_slug_lookup(
    nodes: &HashMap<GeoId, GeoNode>,
) -> Result<HashMap<SlugLookupKey, SlugLookupValue>, RegionNamesError> {
    let mut lookup = HashMap::new();

    for node in nodes.values() {
        for (locale, slug) in &node.slugs {
            insert_slug(
                &mut lookup,
                SlugLookupKey {
                    parent: node.parent.clone(),
                    locale: locale.clone(),
                    slug: slug.clone(),
                },
                SlugLookupValue {
                    region_id: node.id.clone(),
                    canonical: true,
                },
            )?;
        }
    }

    for node in nodes.values() {
        for (locale, aliases) in &node.slug_aliases {
            for alias in aliases {
                let key = SlugLookupKey {
                    parent: node.parent.clone(),
                    locale: locale.clone(),
                    slug: alias.clone(),
                };

                if lookup
                    .get(&key)
                    .is_some_and(|existing| existing.region_id == node.id)
                {
                    continue;
                }

                insert_slug(
                    &mut lookup,
                    key,
                    SlugLookupValue {
                        region_id: node.id.clone(),
                        canonical: false,
                    },
                )?;
            }
        }
    }

    Ok(lookup)
}

fn insert_slug(
    lookup: &mut HashMap<SlugLookupKey, SlugLookupValue>,
    key: SlugLookupKey,
    value: SlugLookupValue,
) -> Result<(), RegionNamesError> {
    if let Some(existing) = lookup.get(&key) {
        if existing.region_id != value.region_id {
            return Err(RegionNamesError::DuplicateSlug {
                parent: key.parent,
                locale: key.locale,
                slug: key.slug,
                first_region: existing.region_id.clone(),
                second_region: value.region_id,
            });
        }
        return Ok(());
    }

    lookup.insert(key, value);
    Ok(())
}

fn push_unique(output: &mut Vec<String>, value: &str) {
    if !output.iter().any(|existing| existing == value) {
        output.push(value.to_string());
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_index() -> GeographyIndex {
        let config = GeographyConfig {
            schema_version: 1,
            fallback_locale: "en".to_string(),
            public_locales: vec![],
            require_public_locale_slugs: false,
            allow_unicode_slugs: true,
        };

        let countries: CountriesFile = toml::from_str(
            r#"
schema_version = 1

[[country]]
id = "BE"
kind = "country"
official_languages = ["nl", "fr", "de"]
[country.names]
en = "Belgium"
nl = "België"
fr = "Belgique"
de = "Belgien"
[country.slugs]
en = "belgium"
nl = "belgie"
fr = "belgique"
de = "belgien"

[[country]]
id = "DE"
kind = "country"
official_languages = ["de"]
[country.names]
en = "Germany"
de = "Deutschland"
[country.slugs]
en = "germany"
de = "deutschland"
"#,
        )
        .unwrap();

        let regions: RegionsFile = toml::from_str(
            r#"
schema_version = 1
parent = "DE"

[[region]]
id = "DE-BY"
kind = "state"
official_languages = ["de"]
[region.names]
en = "Bavaria"
de = "Bayern"
[region.slugs]
en = "bavaria"
de = "bayern"
[region.slug_aliases]
en = ["old-bavaria"]
"#,
        )
        .unwrap();

        GeographyIndex::from_files(config, countries, vec![regions]).unwrap()
    }

    #[test]
    fn localized_path_round_trip() {
        let index = test_index();
        assert_eq!(
            index.localized_path("DE-BY", "de").unwrap(),
            vec!["deutschland", "bayern"]
        );

        let resolved = index
            .resolve_path("de", &["deutschland", "bayern"])
            .unwrap();
        assert_eq!(resolved.region_id, "DE-BY");
        assert!(resolved.canonical);
    }

    #[test]
    fn multilingual_display_names() {
        let index = test_index();
        assert_eq!(
            index.display_names("BE", "en").unwrap(),
            vec!["Belgium", "België", "Belgique", "Belgien"]
        );
        assert_eq!(
            index.display_names("BE", "fr").unwrap(),
            vec!["Belgique", "België", "Belgien"]
        );
    }

    #[test]
    fn alias_is_noncanonical() {
        let index = test_index();
        let resolved = index
            .resolve_path("en", &["germany", "old-bavaria"])
            .unwrap();
        assert_eq!(resolved.region_id, "DE-BY");
        assert!(!resolved.canonical);
    }
}
