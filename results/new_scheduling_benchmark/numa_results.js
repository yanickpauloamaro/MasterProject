let sizes_str = ["4KB", "8KB", "16KB", "32KB", "64KB", "128KB", "256KB", "512KB", "1MB", "2MB", "4MB", "8MB", "16MB", "32MB", "64MB", "128MB"];
let sizes = [4096, 8192, 16384, 32768, 65536, 131072, 262144, 524288, 1048576, 2097152, 4194304, 8388608, 16777216, 33554432, 67108864, 134217728];
let all_core_values = [
	['Core#0', 2.601, 2.598, 2.594, 2.589, 3.451, 4.320, 4.323, 4.368, 4.582, 5.567, 5.495, 5.562, 5.761, 11.203, 14.154, 15.473],
	['Core#1', 2.594, 2.593, 2.593, 2.592, 3.435, 4.307, 4.328, 4.372, 4.519, 5.534, 5.492, 5.565, 5.777, 10.030, 14.470, 14.998],
	['Core#2', 2.592, 2.594, 2.594, 2.592, 3.428, 4.307, 4.334, 4.361, 4.519, 5.535, 5.487, 5.573, 5.803, 10.325, 13.905, 15.530],
	['Core#3', 2.595, 2.596, 2.599, 2.595, 3.438, 4.310, 4.319, 4.372, 4.513, 5.528, 5.485, 5.555, 5.808, 10.281, 14.376, 15.658],
	['Core#4', 2.601, 2.598, 2.604, 2.604, 3.426, 4.310, 4.332, 4.373, 4.522, 5.527, 5.490, 5.577, 5.773, 10.916, 14.056, 15.810],
	['Core#5', 2.595, 2.587, 2.597, 2.595, 3.434, 4.308, 4.330, 4.376, 4.521, 5.536, 5.485, 5.594, 5.791, 11.689, 14.610, 15.048],
	['Core#6', 2.602, 2.592, 2.591, 2.598, 3.444, 4.314, 4.334, 4.379, 4.512, 5.531, 5.489, 5.563, 5.783, 12.639, 14.696, 15.618],
	['Core#7', 2.597, 2.594, 2.608, 2.600, 3.448, 4.304, 4.334, 4.367, 4.536, 5.539, 5.486, 5.556, 5.859, 12.480, 14.923, 15.915],
	['Core#8', 2.598, 2.600, 2.603, 2.592, 3.440, 4.308, 4.342, 4.372, 4.519, 5.539, 5.482, 5.552, 5.820, 12.067, 14.663, 15.874],
	['Core#9', 2.599, 2.596, 2.594, 2.597, 3.459, 4.308, 4.330, 4.365, 4.515, 5.531, 5.491, 5.560, 5.823, 12.586, 14.726, 15.472],
	['Core#10', 2.600, 2.597, 2.602, 2.596, 3.442, 4.310, 4.327, 4.369, 4.510, 5.527, 5.493, 5.573, 5.816, 12.744, 14.691, 15.566],
	['Core#11', 2.603, 2.589, 2.592, 2.598, 3.448, 4.317, 4.329, 4.366, 4.519, 5.541, 5.507, 5.589, 5.984, 12.961, 15.114, 15.597],
	['Core#12', 2.603, 2.592, 2.597, 2.602, 3.440, 4.322, 4.341, 4.375, 4.514, 5.529, 5.498, 5.593, 5.849, 12.645, 15.034, 15.759],
	['Core#13', 2.596, 2.595, 2.596, 2.600, 3.444, 4.310, 4.336, 4.370, 4.517, 5.531, 5.495, 5.572, 5.800, 12.981, 15.049, 15.616],
	['Core#14', 2.602, 2.600, 2.597, 2.594, 3.446, 4.308, 4.333, 4.377, 4.506, 5.529, 5.486, 5.565, 5.801, 12.979, 14.944, 15.459],
	['Core#15', 2.588, 2.592, 2.600, 2.594, 3.440, 4.315, 4.335, 4.368, 4.518, 5.535, 5.504, 5.602, 6.021, 12.649, 14.841, 15.615],
	['Core#16', 2.602, 2.593, 2.598, 2.603, 3.435, 4.307, 4.328, 4.356, 4.513, 5.531, 5.487, 5.572, 5.876, 13.192, 14.655, 15.485],
	['Core#17', 2.587, 2.593, 2.602, 2.592, 3.446, 4.313, 4.337, 4.364, 4.507, 5.532, 5.491, 5.566, 5.863, 12.878, 15.098, 15.887],
	['Core#18', 2.592, 2.594, 2.596, 2.603, 3.439, 4.310, 4.330, 4.361, 4.515, 5.534, 5.487, 5.566, 5.878, 12.813, 14.888, 15.667],
	['Core#19', 2.599, 2.602, 2.591, 2.598, 3.436, 4.309, 4.332, 4.368, 4.510, 5.533, 5.493, 5.575, 5.848, 12.772, 14.937, 14.921],
	['Core#20', 2.590, 2.590, 2.598, 2.604, 3.440, 4.309, 4.338, 4.375, 4.515, 5.541, 5.489, 5.563, 5.885, 12.800, 14.923, 15.638],
	['Core#21', 2.592, 2.605, 2.600, 2.601, 3.446, 4.305, 4.334, 4.363, 4.520, 5.536, 5.484, 5.554, 5.822, 12.443, 14.914, 15.046],
	['Core#22', 2.599, 2.602, 2.599, 2.601, 3.440, 4.315, 4.328, 4.368, 4.520, 5.532, 5.488, 5.585, 6.036, 13.316, 14.890, 15.432],
	['Core#23', 2.589, 2.598, 2.595, 2.601, 3.440, 4.320, 4.336, 4.376, 4.518, 5.535, 5.491, 5.564, 5.907, 12.031, 14.548, 15.599],
	['Core#24', 2.596, 2.591, 2.594, 2.597, 3.444, 4.321, 4.331, 4.368, 4.509, 5.537, 5.482, 5.556, 5.797, 12.427, 14.297, 15.729],
	['Core#25', 2.594, 2.603, 2.598, 2.590, 3.443, 4.313, 4.324, 4.365, 4.516, 5.531, 5.493, 5.558, 5.852, 12.618, 14.415, 15.051],
	['Core#26', 2.593, 2.593, 2.600, 2.588, 3.443, 4.309, 4.339, 4.366, 4.510, 5.539, 5.487, 5.564, 5.993, 10.887, 14.605, 15.075],
	['Core#27', 2.591, 2.595, 2.599, 2.595, 3.433, 4.304, 4.337, 4.365, 4.521, 5.542, 5.481, 5.562, 5.778, 10.800, 14.403, 15.022],
	['Core#28', 2.593, 2.603, 2.611, 2.595, 3.438, 4.310, 4.326, 4.364, 4.514, 5.540, 5.488, 5.547, 5.789, 10.488, 13.971, 15.007],
	['Core#29', 2.607, 2.595, 2.603, 2.591, 3.443, 4.312, 4.332, 4.372, 4.518, 5.542, 5.487, 5.568, 5.877, 11.879, 14.979, 15.764],
	['Core#30', 2.599, 2.596, 2.594, 2.601, 3.434, 4.313, 4.322, 4.362, 4.511, 5.530, 5.480, 5.559, 5.781, 10.265, 14.424, 15.479],
	['Core#31', 2.601, 2.599, 2.593, 2.592, 3.446, 4.306, 4.328, 4.365, 4.507, 5.532, 5.487, 5.568, 5.833, 11.538, 14.853, 15.552],
];
// =======================================================================================================================================
let sizes_str = ["4KB", "8KB", "16KB", "32KB", "64KB", "128KB", "256KB", "512KB", "1MB", "2MB", "4MB", "8MB", "16MB", "32MB", "64MB", "128MB"];
let sizes = [4096, 8192, 16384, 32768, 65536, 131072, 262144, 524288, 1048576, 2097152, 4194304, 8388608, 16777216, 33554432, 67108864, 134217728];
let all_core_values = [
        ['Core#0', 2.599, 2.591, 2.588, 2.592, 3.436, 4.301, 4.311, 4.352, 4.554, 5.569, 5.494, 5.555, 5.801, 10.470, 14.760, 15.057],
        ['Core#1', 2.588, 2.591, 2.585, 2.585, 3.427, 4.305, 4.321, 4.356, 4.657, 5.553, 5.481, 5.575, 5.846, 12.226, 14.537, 14.969],
        ['Core#2', 2.584, 2.591, 2.586, 2.584, 3.423, 4.303, 4.318, 4.361, 4.653, 5.544, 5.476, 5.566, 5.831, 9.776, 13.671, 14.716],
        ['Core#3', 2.582, 2.591, 2.586, 2.586, 3.426, 4.298, 4.326, 4.357, 4.659, 5.547, 5.484, 5.576, 5.822, 9.719, 13.409, 14.715],
        ['Core#4', 2.584, 2.590, 2.593, 2.586, 3.427, 4.297, 4.320, 4.355, 4.660, 5.544, 5.480, 5.549, 5.756, 9.831, 13.899, 14.612],
        ['Core#5', 2.590, 2.587, 2.586, 2.595, 3.433, 4.299, 4.320, 4.353, 4.659, 5.556, 5.487, 5.552, 5.724, 10.284, 13.247, 14.524],
        ['Core#6', 2.589, 2.580, 2.589, 2.582, 3.431, 4.297, 4.323, 4.354, 4.660, 5.541, 5.488, 5.556, 5.823, 11.719, 14.242, 14.909],
        ['Core#7', 2.589, 2.582, 2.586, 2.584, 3.428, 4.298, 4.320, 4.356, 4.660, 5.542, 5.490, 5.564, 5.753, 10.978, 13.608, 14.777],
        ['Core#8', 2.585, 2.587, 2.584, 2.582, 3.426, 4.297, 4.320, 4.357, 4.664, 5.542, 5.492, 5.557, 5.729, 9.165, 13.620, 14.729],
        ['Core#9', 2.593, 2.584, 2.586, 2.586, 3.434, 4.299, 4.321, 4.355, 4.663, 5.546, 5.488, 5.546, 5.712, 9.574, 14.246, 14.706],
        ['Core#10', 2.593, 2.582, 2.586, 2.583, 3.435, 4.298, 4.317, 4.356, 4.659, 5.536, 5.479, 5.559, 5.708, 9.082, 13.674, 14.841],
        ['Core#11', 2.588, 2.587, 2.582, 2.589, 3.432, 4.298, 4.320, 4.352, 4.663, 5.542, 5.474, 5.554, 5.781, 11.199, 14.521, 14.804],
        ['Core#12', 2.588, 2.588, 2.583, 2.597, 3.428, 4.304, 4.317, 4.362, 4.660, 5.547, 5.472, 5.549, 5.739, 10.888, 14.368, 15.357],
        ['Core#13', 2.587, 2.584, 2.582, 2.587, 3.432, 4.299, 4.320, 4.358, 4.661, 5.558, 5.481, 5.560, 5.833, 11.521, 14.429, 14.906],
        ['Core#14', 2.590, 2.585, 2.588, 2.592, 3.426, 4.299, 4.322, 4.363, 4.660, 5.551, 5.484, 5.573, 5.770, 10.177, 13.908, 15.010],
        ['Core#15', 2.587, 2.589, 2.586, 2.587, 3.429, 4.302, 4.317, 4.357, 4.660, 5.550, 5.475, 5.580, 5.847, 10.753, 13.618, 14.740],
        ['Core#16', 2.586, 2.586, 2.590, 2.585, 3.426, 4.297, 4.316, 4.354, 4.656, 5.556, 5.484, 5.560, 5.811, 10.316, 13.379, 14.636],
        ['Core#17', 2.588, 2.586, 2.594, 2.591, 3.426, 4.299, 4.321, 4.359, 4.659, 5.561, 5.480, 5.568, 5.760, 10.843, 14.043, 14.905],
        ['Core#18', 2.590, 2.587, 2.585, 2.586, 3.436, 4.298, 4.319, 4.359, 4.659, 5.553, 5.479, 5.562, 5.739, 9.899, 13.831, 14.603],
        ['Core#19', 2.589, 2.589, 2.586, 2.591, 3.438, 4.300, 4.318, 4.357, 4.657, 5.549, 5.481, 5.561, 5.791, 11.586, 14.308, 14.665],
        ['Core#20', 2.585, 2.585, 2.589, 2.587, 3.433, 4.303, 4.319, 4.357, 4.663, 5.539, 5.482, 5.561, 5.709, 8.213, 13.821, 15.049],
        ['Core#21', 2.592, 2.583, 2.589, 2.584, 3.437, 4.300, 4.323, 4.361, 4.667, 5.539, 5.479, 5.546, 5.752, 9.070, 13.515, 15.161],
        ['Core#22', 2.587, 2.587, 2.590, 2.588, 3.429, 4.299, 4.323, 4.359, 4.664, 5.538, 5.474, 5.548, 5.728, 8.782, 13.835, 14.832],
        ['Core#23', 2.590, 2.583, 2.586, 2.585, 3.435, 4.297, 4.323, 4.356, 4.666, 5.535, 5.478, 5.552, 5.743, 9.424, 14.223, 14.816],
        ['Core#24', 2.591, 2.584, 2.585, 2.586, 3.430, 4.301, 4.321, 4.358, 4.663, 5.546, 5.488, 5.560, 5.722, 9.388, 13.799, 14.874],
        ['Core#25', 2.592, 2.622, 2.603, 2.595, 3.431, 4.298, 4.322, 4.369, 4.669, 5.550, 5.493, 5.558, 5.727, 9.306, 14.046, 16.192],
        ['Core#26', 2.600, 2.592, 2.596, 2.590, 3.435, 4.301, 4.323, 4.357, 4.655, 5.540, 5.477, 5.555, 5.746, 9.826, 15.028, 14.901],
        ['Core#27', 2.594, 2.590, 2.595, 2.592, 3.428, 4.299, 4.322, 4.357, 4.657, 5.538, 5.480, 5.566, 5.782, 11.146, 14.625, 15.144],
        ['Core#28', 2.593, 2.593, 2.591, 2.592, 3.433, 4.303, 4.322, 4.357, 4.663, 5.543, 5.476, 5.561, 5.798, 11.132, 14.052, 15.110],
        ['Core#29', 2.595, 2.590, 2.593, 2.591, 3.435, 4.297, 4.324, 4.362, 4.662, 5.549, 5.483, 5.550, 5.801, 10.174, 14.388, 14.983],
        ['Core#30', 2.592, 2.594, 2.589, 2.594, 3.430, 4.306, 4.324, 4.365, 4.655, 5.546, 5.479, 5.562, 5.803, 10.753, 13.923, 14.925],
        ['Core#31', 2.587, 2.590, 2.586, 2.592, 3.435, 4.301, 4.321, 4.362, 4.666, 5.555, 5.489, 5.576, 5.792, 10.220, 13.807, 15.220],
];

// =======================================================================================================================================
// Fait au moins 100 iteration
let sizes_str = ["4KB", "8KB", "16KB", "32KB", "64KB", "128KB", "256KB", "512KB", "1MB", "2MB", "4MB", "8MB", "16MB", "32MB", "64MB", "128MB"];
let sizes = [4096, 8192, 16384, 32768, 65536, 131072, 262144, 524288, 1048576, 2097152, 4194304, 8388608, 16777216, 33554432, 67108864, 134217728];
let all_core_values = [
        ['Core#0', 2.607, 2.593, 2.590, 2.594, 3.441, 4.302, 4.324, 4.357, 4.640, 5.544, 5.492, 5.570, 5.765, 11.356, 13.926, 14.888],
        ['Core#1', 2.592, 2.597, 2.594, 2.593, 3.436, 4.303, 4.323, 4.361, 4.609, 5.524, 5.483, 5.571, 5.676, 8.427, 13.392, 15.038],
        ['Core#2', 2.602, 2.589, 2.595, 2.587, 3.437, 4.305, 4.324, 4.361, 4.604, 5.525, 5.484, 5.565, 5.733, 12.003, 13.588, 14.979],
        ['Core#3', 2.594, 2.588, 2.600, 2.589, 3.429, 4.309, 4.327, 4.363, 4.608, 5.526, 5.485, 5.555, 5.690, 9.707, 13.781, 15.197],
        ['Core#4', 2.587, 2.601, 2.595, 2.591, 3.441, 4.304, 4.322, 4.357, 4.612, 5.529, 5.485, 5.565, 5.737, 10.334, 13.504, 14.958],
        ['Core#5', 2.593, 2.589, 2.589, 2.595, 3.432, 4.307, 4.326, 4.358, 4.606, 5.541, 5.491, 5.554, 5.707, 9.045, 13.976, 15.032],
        ['Core#6', 2.591, 2.594, 2.593, 2.589, 3.438, 4.302, 4.324, 4.357, 4.608, 5.532, 5.485, 5.564, 5.744, 9.926, 13.349, 14.914],
        ['Core#7', 2.600, 2.593, 2.593, 2.594, 3.436, 4.301, 4.337, 4.359, 4.607, 5.517, 5.484, 5.558, 5.672, 8.074, 13.664, 14.964],
        ['Core#8', 2.594, 2.595, 2.596, 2.596, 3.436, 4.304, 4.326, 4.358, 4.606, 5.533, 5.485, 5.571, 5.739, 11.078, 13.852, 14.894],
        ['Core#9', 2.594, 2.592, 2.588, 2.588, 3.437, 4.310, 4.325, 4.359, 4.604, 5.521, 5.487, 5.560, 5.732, 9.436, 13.796, 15.060],
        ['Core#10', 2.596, 2.595, 2.597, 2.596, 3.438, 4.305, 4.326, 4.365, 4.608, 5.528, 5.489, 5.572, 5.734, 10.091, 13.836, 14.942],
        ['Core#11', 2.595, 2.595, 2.595, 2.595, 3.434, 4.313, 4.322, 4.362, 4.604, 5.537, 5.485, 5.564, 5.710, 9.415, 13.959, 14.934],
        ['Core#12', 2.589, 2.593, 2.593, 2.590, 3.430, 4.310, 4.337, 4.360, 4.608, 5.534, 5.490, 5.562, 5.743, 8.697, 13.411, 14.866],
        ['Core#13', 2.590, 2.594, 2.594, 2.594, 3.428, 4.305, 4.327, 4.362, 4.614, 5.525, 5.494, 5.593, 5.890, 10.671, 13.893, 15.034],
        ['Core#14', 2.593, 2.588, 2.598, 2.600, 3.437, 4.306, 4.325, 4.361, 4.606, 5.524, 5.484, 5.569, 5.723, 8.783, 13.541, 14.818],
        ['Core#15', 2.595, 2.594, 2.595, 2.597, 3.431, 4.305, 4.334, 4.360, 4.611, 5.527, 5.492, 5.551, 5.707, 9.124, 13.892, 14.825],
        ['Core#16', 2.596, 2.590, 2.594, 2.598, 3.453, 4.297, 4.326, 4.357, 4.614, 5.534, 5.492, 5.572, 5.713, 9.432, 13.713, 14.904],
        ['Core#17', 2.593, 2.592, 2.600, 2.589, 3.432, 4.307, 4.325, 4.363, 4.611, 5.525, 5.487, 5.577, 5.723, 9.549, 14.279, 15.033],
        ['Core#18', 2.591, 2.601, 2.600, 2.589, 3.430, 4.302, 4.324, 4.362, 4.611, 5.535, 5.486, 5.568, 5.742, 10.361, 13.940, 15.042],
        ['Core#19', 2.589, 2.596, 2.592, 2.592, 3.435, 4.304, 4.322, 4.363, 4.610, 5.537, 5.489, 5.563, 5.672, 10.084, 14.127, 15.043],
        ['Core#20', 2.592, 2.595, 2.595, 2.596, 3.442, 4.306, 4.328, 4.372, 4.608, 5.532, 5.487, 5.568, 5.698, 9.745, 13.550, 14.757],
        ['Core#21', 2.588, 2.590, 2.598, 2.594, 3.430, 4.302, 4.326, 4.360, 4.601, 5.527, 5.472, 5.555, 5.706, 9.929, 14.138, 14.935],
        ['Core#22', 2.595, 2.595, 2.600, 2.591, 3.431, 4.301, 4.323, 4.361, 4.609, 5.526, 5.494, 5.554, 5.723, 10.004, 13.958, 14.902],
        ['Core#23', 2.595, 2.592, 2.592, 2.592, 3.435, 4.303, 4.323, 4.366, 4.604, 5.529, 5.477, 5.559, 5.691, 9.147, 14.173, 14.869],
        ['Core#24', 2.591, 2.596, 2.596, 2.592, 3.425, 4.305, 4.328, 4.358, 4.606, 5.524, 5.486, 5.566, 5.716, 8.801, 13.819, 14.791],
        ['Core#25', 2.600, 2.591, 2.590, 2.592, 3.436, 4.303, 4.326, 4.355, 4.610, 5.522, 5.485, 5.565, 5.702, 9.956, 14.152, 15.049],
        ['Core#26', 2.589, 2.597, 2.589, 2.589, 3.436, 4.304, 4.322, 4.355, 4.602, 5.525, 5.484, 5.558, 5.718, 9.175, 13.320, 14.921],
        ['Core#27', 2.591, 2.595, 2.590, 2.596, 3.430, 4.301, 4.322, 4.361, 4.603, 5.527, 5.476, 5.555, 5.680, 8.401, 13.878, 14.704],
        ['Core#28', 2.595, 2.589, 2.590, 2.589, 3.429, 4.300, 4.322, 4.359, 4.611, 5.523, 5.485, 5.556, 5.754, 11.395, 14.200, 15.117],
        ['Core#29', 2.595, 2.588, 2.585, 2.596, 3.433, 4.302, 4.319, 4.364, 4.603, 5.527, 5.494, 5.557, 5.825, 11.216, 14.169, 14.949],
        ['Core#30', 2.588, 2.592, 2.592, 2.591, 3.432, 4.301, 4.325, 4.355, 4.605, 5.533, 5.482, 5.555, 5.734, 10.056, 13.912, 15.034],
        ['Core#31', 2.589, 2.592, 2.595, 2.595, 3.434, 4.303, 4.321, 4.362, 4.601, 5.532, 5.485, 5.548, 5.744, 11.745, 14.771, 15.033],
];

// =======================================================================================================================================
// Only large sizes with all iterations
let sizes_str = ["8MB", "16MB", "32MB", "64MB", "128MB"];
let sizes = [8388608, 16777216, 33554432, 67108864, 134217728];
let all_core_values = [
	['Core#0', 5.658, 5.818, 11.519, 14.122, 15.059],
	['Core#1', 5.664, 5.857, 10.510, 14.099, 14.967],
	['Core#2', 5.666, 5.873, 10.747, 13.905, 15.033],
	['Core#3', 5.655, 5.867, 10.534, 14.006, 15.044],
	['Core#4', 5.662, 5.902, 10.676, 14.170, 15.084],
	['Core#5', 5.690, 5.896, 9.591, 14.158, 14.877],
	['Core#6', 5.680, 5.989, 10.642, 14.275, 15.040],
	['Core#7', 5.677, 5.881, 10.574, 14.385, 14.993],
	['Core#8', 5.668, 5.892, 10.698, 14.010, 14.999],
	['Core#9', 5.654, 5.871, 11.020, 14.069, 15.130],
	['Core#10', 5.654, 5.903, 11.170, 14.049, 15.094],
	['Core#11', 5.661, 6.012, 11.181, 14.387, 15.249],
	['Core#12', 5.669, 5.941, 10.417, 14.242, 14.999],
	['Core#13', 5.663, 5.916, 10.436, 13.922, 14.966],
	['Core#14', 5.667, 5.879, 10.270, 14.542, 15.217],
	['Core#15', 5.669, 5.881, 10.793, 14.262, 15.099],
	['Core#16', 5.675, 5.876, 11.164, 14.219, 15.143],
	['Core#17', 5.668, 5.896, 11.052, 14.285, 15.144],
	['Core#18', 5.659, 5.972, 10.187, 14.225, 15.059],
	['Core#19', 5.643, 5.972, 11.117, 14.404, 15.089],
	['Core#20', 5.669, 5.968, 10.300, 14.618, 14.996],
	['Core#21', 5.664, 5.866, 9.663, 14.152, 15.069],
	['Core#22', 5.701, 5.866, 11.327, 14.342, 15.060],
	['Core#23', 5.652, 5.870, 10.918, 14.245, 15.067],
	['Core#24', 5.638, 5.894, 10.758, 14.037, 15.250],
	['Core#25', 5.674, 5.978, 11.738, 14.186, 15.053],
	['Core#26', 5.677, 5.963, 10.977, 14.374, 15.196],
	['Core#27', 5.646, 5.943, 11.248, 14.282, 15.126],
	['Core#28', 5.690, 5.911, 10.629, 14.506, 15.229],
	['Core#29', 5.675, 5.922, 11.202, 14.196, 15.283],
	['Core#30', 5.690, 5.868, 10.627, 14.000, 15.095],
	['Core#31', 5.662, 5.861, 9.949, 14.094, 15.073],
];