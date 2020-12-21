
pub const DEFAULT_GEOMETRY_STYLE:&str = r#"
{
    "boundary_relations": true,
    "feature_keys": [
        "aerialway",
        "aeroway",
        "amenity",
        "barrier",
        "boundary",
        "bridge",
        "building",
        "construction",
        "embankment",
        "highway",
        "historic",
        "junction",
        "landuse",
        "leisure",
        "lock",
        "man_made",
        "military",
        "natural",
        "place",
        "power",
        "railway",
        "route",
        "service",
        "shop",
        "tourism",
        "tunnel",
        "water",
        "waterway"
    ],
    "multipolygons": true,
    "other_keys": null,
    "parent_tags": {
        "parent_highway": {
            "node_keys": [
                "highway",
                "railway"
            ],
            "way_key": "highway",
            "way_priority": {
                "bridleway": 2,
                "byway": 8,
                "cycleway": 1,
                "footway": 0,
                "living_street": 9,
                "motorway": 21,
                "motorway_link": 22,
                "path": 3,
                "pedestrian": 5,
                "primary": 17,
                "primary_link": 18,
                "rail": 24,
                "residential": 10,
                "road": 11,
                "secondary": 15,
                "secondary_link": 16,
                "service": 6,
                "siding": 23,
                "steps": 4,
                "tertiary": 13,
                "tertiary_link": 14,
                "track": 7,
                "trunk": 19,
                "trunk_link": 20,
                "unclassified": 12
            }
        },
        "parent_service": {
            "node_keys": [
                "highway"
            ],
            "way_key": "service",
            "way_priority": {}
        }
    },
    "polygon_tags": {
        "aeroway": {
            "exclude": [
                "taxiway"
            ]
        },
        "amenity": "all",
        "area": "all",
        "area:highway": "all",
        "barrier": {
            "include": [
                "city_wall",
                "ditch",
                "wall",
                "spikes"
            ]
        },
        "boundary": "all",
        "building": "all",
        "building:part": "all",
        "golf": "all",
        "highway": {
            "include": [
                "services",
                "rest_area",
                "escape",
                "elevator"
            ]
        },
        "historic": "all",
        "landuse": "all",
        "leisure": "all",
        "man_made": {
            "exclude": [
                "cutline",
                "embankment",
                "pipeline"
            ]
        },
        "military": "all",
        "natural": {
            "exclude": [
                "coastline",
                "cliff",
                "ridge",
                "arete",
                "tree_row"
            ]
        },
        "office": "all",
        "place": "all",
        "power": {
            "include": [
                "plant",
                "substation",
                "generator",
                "transformer"
            ]
        },
        "public_transport": "all",
        "railway": {
            "include": [
                "station",
                "turntable",
                "roundhouse",
                "platform"
            ]
        },
        "shop": "all",
        "tourism": "all",
        "waterway": {
            "include": [
                "riverbank",
                "dock",
                "boatyard",
                "dam"
            ]
        }
    },
    "relation_tag_spec": [
        {
            "source_filter": {
                "boundary": "administrative",
                "type": "boundary"
            },
            "source_key": "admin_level",
            "target_key": "min_admin_level",
            "type": "min"
        },
        {
            "source_filter": {
                "boundary": "administrative",
                "type": "boundary"
            },
            "source_key": "admin_level",
            "target_key": "max_admin_level",
            "type": "max"
        },
        {
            "source_filter": {
                "route": "bus",
                "type": "route"
            },
            "source_key": "ref",
            "target_key": "bus_routes",
            "type": "list"
        },
        {
            "source_filter": {
                "route": "bicycle",
                "type": "route"
            },
            "source_key": "ref",
            "target_key": "bicycle_routes",
            "type": "list"
        }
    ]
}
"#;
