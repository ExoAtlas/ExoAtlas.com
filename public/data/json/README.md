EXOATLAS OBJECT ID NOMENCLATURE

All objects in the ExoAtlas dataset are assigned a unique, 9-character numeric identifier (objnum) for consistent data management. The ID's structure is hierarchical and chronological, revealing an object's category and its relationship to other bodies.

ID Structure Breakdown
The objnum is a 9-character string composed of two main parts: a prefix and a unique ID. The prefix defines the object's category, while the unique ID identifies the specific object within that category. The system uses zero-padding to maintain the 9-character length.

Prefix	Category	Description
0	Star System	For the Sun, which is the central body.
1	Planets & Dwarf Planets	For planets and dwarf planets, ordered by their distance from the Sun.
2	Moons	For moons, which are numbered hierarchically based on their parent planet.
3	Asteroids	For asteroids, using their official Minor Planet Center number.
4	Comets	For comets, using their official periodic comet number.
5	Spacecraft	For spacecraft, using their NORAD ID.
6-9	Reserved	For future use.

Export to Sheets
Chronological and Hierarchical Identification
Star System (Prefix 0)
The Sun is the central body of the solar system and is given a unique 9-digit identifier.

Structure: 0 + 00000000

Example (The Sun): 000000000

Planets & Dwarf Planets (Prefix 1)
Planets and dwarf planets are assigned a number chronologically based on their distance from the Sun. The remaining digits are 0s to distinguish them as a primary body.

Structure: 1 + [Planet ID] + 000000

Planet ID: A two-digit number (01, 02, etc.) representing the planet's order from the Sun.

Example (Mercury): 1 + 01 + 000000 = 101000000

Example (Earth): 1 + 03 + 000000 = 103000000

Moons (Prefix 2)
Moons are identified hierarchically based on their parent planet and chronologically by their discovery within that system.

Structure: 2 + [Planet ID] + [Moon ID]

Planet ID: A two-digit number (01, 02, etc.) representing the parent planet's order from the Sun.

Moon ID: A four-digit number representing the moon's sequential number within the parent system. This number can be assigned chronologically by discovery or by a formal IAU designation.

Example (The Moon): 2 + 03 + 0001 = 20300001 (Earth is the 3rd planet, and the Moon is its 1st moon).

Example (Phobos, a Martian moon): 2 + 04 + 0001 = 20400001 (Mars is the 4th planet, and Phobos is its 1st moon by discovery).

Asteroids (Prefix 3)
Asteroids use their official Minor Planet Center number.

Structure: 3 + [Minor Planet Center Number]

Example (Ceres, number 1): 3 + 00000001 = 300000001

Comets (Prefix 4)
Comets use their official periodic comet number or an internal ID if a number is not yet assigned.

Structure: 4 + [Periodic Comet Number]

Example (Halley's Comet, number 1P): 4 + 00000001 = 400000001

Spacecraft (Prefix 5)
Spacecraft are identified using their official NORAD ID, padded to maintain the 9-character structure.

Structure: 5 + [NORAD ID]

Example (ISS, NORAD ID 25544): 5 + 00025544 = 500025544

Special Cases
Virtual locations, such as Lagrange points, are not assigned an objnum. They are referenced by a separate location ID that uses the planet ID and a reserved series of numbers for the specific point.

Example (Sun-Earth L1 Point): 103 + 9001 = 1039001 (This is not a full 9-digit objnum, but a separate location ID that links to Earth, the 3rd planet).

