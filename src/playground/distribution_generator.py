import random
from collections import Counter
import numpy as np
from itertools import combinations
import matplotlib.pyplot as plt

def create_color_distribution():
    # Total colors and their counts
    color_counts = {
        'Red': 300,
        'Blue': 278,
        'Green': 215,
        'Yellow': 192,
        'Purple': 188,
        'Orange': 187,
        'Cyan': 143,
        'Magenta': 121,
        'Indigo': 99,
        'Teal': 87
    }

    # Calculate total cells
    total_cells = sum(color_counts.values())

    # Number of boxes
    num_boxes = 250

    # Initialize each box with 1 cell
    cells_per_box = [1] * num_boxes

    # Remaining cells to distribute
    remaining = total_cells - num_boxes

    # Distribute remaining cells randomly, ensuring no box exceeds 10 cells
    while remaining > 0:
        eligible_boxes = [i for i in range(num_boxes) if cells_per_box[i] < 10]
        if not eligible_boxes:
            raise ValueError("Cannot distribute cells without exceeding max per box")
        box_idx = random.choice(eligible_boxes)
        cells_per_box[box_idx] += 1
        remaining -= 1

    # Create the list of all colors
    all_colors = []
    for color, count in color_counts.items():
        all_colors.extend([color] * count)

    # Shuffle until all boxes have arrangable colors (no color freq > (num+1)//2)
    shuffled_good = False
    attempts = 0
    while not shuffled_good and attempts < 1000:
        random.shuffle(all_colors)
        shuffled_good = True
        start = 0
        for num_cells in cells_per_box:
            end = start + num_cells
            box_colors = all_colors[start:end]
            counter = Counter(box_colors)
            if any(v > (num_cells + 1) // 2 for v in counter.values()):
                shuffled_good = False
                break
            start = end
        attempts += 1

    if not shuffled_good:
        raise ValueError("Could not find a distribution where all boxes can be arranged without adjacent same colors")

    # Create the structure: list of lists, arranging each box
    boxes = []
    start = 0
    for num_cells in cells_per_box:
        end = start + num_cells
        box_colors = all_colors[start:end][:]  # copy
        # Shuffle until no adjacent same colors
        attempts = 0
        while attempts < 1000:
            random.shuffle(box_colors)
            if all(box_colors[i] != box_colors[i + 1] for i in range(num_cells - 1)):
                break
            attempts += 1
        else:
            raise ValueError("Could not arrange colors in box without adjacent same colors")
        boxes.append(box_colors)
        start = end
    return boxes

def compute_cooccurrence_matrix(boxes):
    # Get unique colors
    unique_colors = ['Red', 'Blue', 'Green', 'Yellow', 'Purple', 'Orange', 'Cyan', 'Magenta', 'Indigo', 'Teal']
    matrix_size = len(unique_colors)

    # Initialize co-occurrence matrix
    cooccurrence_matrix = np.zeros((matrix_size, matrix_size), dtype=int)

    # Map colors to matrix indices
    color_to_index = {color: idx for idx, color in enumerate(unique_colors)}

    # Count co-occurrences
    for box in boxes:
        # Get all pairs of colors in the box
        for color1, color2 in combinations(set(box), 2):
            i1, i2 = color_to_index[color1], color_to_index[color2]
            cooccurrence_matrix[i1, i2] += 1
            cooccurrence_matrix[i2, i1] += 1  # Symmetric matrix

    return unique_colors, cooccurrence_matrix

# Generate 1000 box structures and their co-occurrence matrices
num_simulations = 5000
unique_colors = ['Red', 'Blue', 'Green', 'Yellow', 'Purple', 'Orange', 'Cyan', 'Magenta', 'Indigo', 'Teal']
matrix_size = len(unique_colors)
all_matrices = np.zeros((num_simulations, matrix_size, matrix_size), dtype=float)

try:
    for sim in range(num_simulations):
        print(f"Generating box structure {sim + 1}/{num_simulations}...")
        boxes = create_color_distribution()
        _, cooccurrence_matrix = compute_cooccurrence_matrix(boxes)
        all_matrices[sim] = cooccurrence_matrix

    # Create a 10x10 grid of subplots for the lower triangle
    fig, axes = plt.subplots(matrix_size, matrix_size, figsize=(20, 20))
    plt.subplots_adjust(wspace=0.3, hspace=0.3)

    # Define color mappings for text
    color_map = {
        'Red': 'red',
        'Blue': 'blue',
        'Green': 'green',
        'Yellow': 'yellow',
        'Purple': 'purple',
        'Orange': 'orange',
        'Cyan': 'cyan',
        'Magenta': 'magenta',
        'Indigo': '#4B0082',  # Dark blue-violet for Indigo
        'Teal': 'teal'
    }

    # Set color labels on axes
    for ax, color in zip(axes[0], unique_colors):
        ax.set_title(color, rotation=45, ha='right', fontsize=10, color=color_map[color])
    for ax, color in zip(axes[:, 0], unique_colors):
        ax.set_ylabel(color, rotation=0, ha='right', fontsize=10, color=color_map[color])

    for i in range(matrix_size):
        for j in range(matrix_size):
            ax = axes[i, j]
            if i > j:  # Lower triangle (excluding diagonal)
                # Extract co-occurrence vector for position (i, j)
                cooccurrences = all_matrices[:, i, j]
                # Plot histogram with prettier styling
                ax.hist(cooccurrences, bins=20, color='#1f77b4', edgecolor='white', alpha=0.8)
                ax.grid(True, linestyle='--', alpha=0.7, color='gray')
                ax.tick_params(axis='both', which='major', labelsize=8)
            else:
                # Remove axes for upper triangle and diagonal
                ax.axis('off')

    # Add overall title
    fig.suptitle('Distributions of Co-occurrences Across 1000 Simulations', fontsize=16, y=1.02)
    # Save the plot
    plt.savefig('cooccurrence_distributions.png', dpi=300, bbox_inches='tight')
    plt.show()

except ValueError as e:
    print(f"Error: {e}")