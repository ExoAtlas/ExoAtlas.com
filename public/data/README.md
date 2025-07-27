ExoAtlas Object ID Nomenclature
All objects in the ExoAtlas dataset are assigned a unique, 9-character numeric identifier (objnum) for consistent data management. The ID's structure reveals the object's category and, where applicable, its relationship to other bodies.

The first digit of the ID is a prefix that defines the object's category.

Prefix	Category
0	Major Body Systems (Planets, Moons)
1	Asteroids
2	Comets
3	Spacecraft

Export to Sheets
Major Body Systems (Prefix 0)
This ID is a 9-character string composed of a prefix, a 3-digit System ID, and a 5-digit Body ID. The primary body of a system (like a planet or the Sun) is assigned a Body ID of 00000.

Structure: [Prefix][System ID][Body ID]

Example (Earth): 0 + 003 + 00000 = 000300000

Example (The Moon): 0 + 003 + 00001 = 000300001

Asteroids (Prefix 1)
This ID consists of the prefix 1 followed by the official Minor Planet Center number, padded with leading zeros to 8 digits.

Structure: [Prefix][Padded Asteroid Number]

Example (Ceres): 1 + 00000001 = 100000001

Comets (Prefix 2)
This ID consists of the prefix 2 followed by the periodic comet number (or an internal ID), padded to 8 digits. The official alphanumeric name (e.g., "1P/Halley") is stored in a separate designation field.

Structure: [Prefix][Padded Comet Number]

Example (Halley's Comet): 2 + 00000001 = 200000001

Spacecraft (Prefix 3)
This ID consists of the prefix 3 followed by the NORAD ID, padded to 8 digits. A spacecraft's location is stored as a separate, dynamic property.

Structure: [Prefix][Padded NORAD ID]

Example (ISS): 3 + 00025544 = 300025544

Special Cases: Location IDs
Virtual locations like Lagrange points are not assigned their own objnum. Instead, they are referenced by a location ID that follows the Major Body System structure. We reserve the 90000 series in the Body ID segment for these points.

Example (Sun-Earth L1 Point): 000390001

Example (Sun-Earth L2 Point): 000390002