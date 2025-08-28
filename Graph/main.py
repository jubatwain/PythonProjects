import matplotlib.pyplot as plt
import numpy as np


def main():
    # Get user input for x and y data
    print("Enter x values separated by commas (e.g., 1,2,3):")
    x_input = input().strip()
    print("Enter y values separated by commas (e.g., 4,5,6):")
    y_input = input().strip()

    # Parse the inputs into lists of floats
    try:
        x = np.array([float(val) for val in x_input.split(',')])
        y = np.array([float(val) for val in y_input.split(',')])

        if len(x) != len(y):
            print("Error: x and y must have the same number of values.")
            return
    except ValueError:
        print("Error: Invalid input. Please enter numeric values separated by commas.")
        return

    # Create the plot with black background
    fig, ax = plt.subplots()
    fig.patch.set_facecolor('black')  # Set figure background to black
    ax.set_facecolor('black')  # Set axes background to black

    # Plot the data as a line graph with white line for visibility
    ax.plot(x, y, color='white', marker='o', linestyle='-', linewidth=2, markersize=6)

    # Set labels and title with white text
    ax.set_xlabel('X-axis', color='white')
    ax.set_ylabel('Y-axis', color='white')
    ax.set_title('User Data Graph', color='white')

    # Set axes spines to white
    ax.spines['bottom'].set_color('white')
    ax.spines['top'].set_color('white')
    ax.spines['left'].set_color('white')
    ax.spines['right'].set_color('white')

    # Set tick colors to white
    ax.tick_params(axis='x', colors='white')
    ax.tick_params(axis='y', colors='white')

    # Display the plot
    plt.show()


if __name__ == "__main__":
    main()