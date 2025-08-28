import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import argparse


def parse_input(input_str):
    if not input_str:
        raise ValueError("Input cannot be empty.")
    try:
        return np.array([float(val) for val in input_str.split(',') if val.strip()])
    except ValueError:
        raise ValueError("Invalid input. Please enter numeric values separated by commas.")


def read_csv_data(filename):
    try:
        df = pd.read_csv(filename)
        if 'x' not in df.columns:
            raise ValueError("CSV file must contain an 'x' column.")
        x = df['x'].values
        y_datasets = [df[col].values for col in df.columns if col != 'x']
        for y in y_datasets:
            if len(y) != len(x):
                raise ValueError("All columns in CSV must have the same length as 'x'.")
        return x, y_datasets
    except Exception as e:
        raise ValueError(f"Error reading CSV file: {e}")


def plot_graph(x, y_datasets, plot_type, save_file=None, show_grid=False):
    max_points = 10000
    if len(x) > max_points:
        print(f"Warning: Dataset is large ({len(x)} points). Only plotting first {max_points} points.")
        x = x[:max_points]
        y_datasets = [y[:max_points] for y in y_datasets]

    fig, ax = plt.subplots()
    fig.patch.set_facecolor('black')
    ax.set_facecolor('black')

    colors = ['white', 'red', 'blue', 'green', 'yellow']
    for i, y in enumerate(y_datasets):
        label = f'Series {i + 1}'
        if plot_type.lower() == 'line':
            ax.plot(x, y, color=colors[i % len(colors)], marker='o', linestyle='-', linewidth=2, markersize=6,
                    label=label)
        elif plot_type.lower() == 'scatter':
            ax.scatter(x, y, color=colors[i % len(colors)], s=50, label=label)
        elif plot_type.lower() == 'bar':
            ax.bar(x + i * 0.2, y, width=0.2, color=colors[i % len(colors)], label=label)
        else:
            print("Unsupported plot type. Using line plot.")
            ax.plot(x, y, color=colors[i % len(colors)], marker='o', linestyle='-', linewidth=2, markersize=6,
                    label=label)

    ax.set_xlabel('X-axis', color='white')
    ax.set_ylabel('Y-axis', color='white')
    ax.set_title('User Data Graph', color='white')
    ax.spines['bottom'].set_color('white')
    ax.spines['top'].set_color('white')
    ax.spines['left'].set_color('white')
    ax.spines['right'].set_color('white')
    ax.tick_params(axis='x', colors='white')
    ax.tick_params(axis='y', colors='white')

    if show_grid:
        ax.grid(True, color='gray', linestyle='--', alpha=0.5)

    ax.legend(facecolor='black', edgecolor='white', labelcolor='white')

    if save_file:
        try:
            plt.savefig(save_file, facecolor='black', bbox_inches='tight')
            print(f"Graph saved as {save_file}")
        except Exception as e:
            print(f"Error saving file: {e}")

    plt.show()


def main():
    parser = argparse.ArgumentParser(description="Plot a graph based on user data.")
    parser.add_argument('--x', type=str, help='X values separated by commas (e.g., 1,2,3)')
    parser.add_argument('--y', type=str, action='append',
                        help='Y values for a dataset separated by commas (e.g., 4,5,6). Can be specified multiple times.')
    parser.add_argument('--csv', type=str, help='CSV file with x and y columns')
    parser.add_argument('--plot-type', type=str, default='line', help='Plot type: line, scatter, bar (default: line)')
    parser.add_argument('--save-file', type=str, help='Filename to save the graph (e.g., graph.png)')
    parser.add_argument('--grid', action='store_true', help='Show grid on the graph')

    args = parser.parse_args()

    try:
        if args.csv:
            x, y_datasets = read_csv_data(args.csv)
        elif args.x and args.y:
            x = parse_input(args.x)
            y_datasets = [parse_input(y) for y in args.y]
            for y in y_datasets:
                if len(y) != len(x):
                    print("Error: Each y dataset must have the same number of values as x.")
                    return
        else:
            print("Enter x values separated by commas (e.g., 1,2,3):")
            x_input = input().strip()
            x = parse_input(x_input)

            y_datasets = []
            while True:
                print("Enter y values for a dataset separated by commas (e.g., 4,5,6). Enter 'done' to finish:")
                y_input = input().strip()
                if y_input.lower() == 'done':
                    if not y_datasets:
                        print("Error: At least one dataset is required.")
                        return
                    break
                y = parse_input(y_input)
                if len(y) != len(x):
                    print("Error: Each y dataset must have the same number of values as x.")
                    continue
                y_datasets.append(y)

        plot_type = args.plot_type if args.plot_type else input(
            "Choose plot type (line, scatter, bar): ").strip() or 'line'
        show_grid = args.grid if args.grid else input("Show grid? (y/n): ").strip().lower() == 'y'
        save_file = args.save_file if args.save_file else input(
            "Save graph to file? Enter filename (e.g., graph.png) or leave blank to skip: ").strip() or None

        plot_graph(x, y_datasets, plot_type, save_file, show_grid)
    except ValueError as e:
        print(f"Error: {e}")


if __name__ == "__main__":
    main()