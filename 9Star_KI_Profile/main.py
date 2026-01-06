import datetime
from typing import Dict, Tuple

class NineStarKiCalculator:
    def __init__(self):
        # Element associations for each number
        self.elements = {
            1: ("Water", "The Diplomat", "Introspective, wise, flexible"),
            2: ("Earth", "The Nurturer", "Gentle, supportive, detail-oriented"),
            3: ("Wood", "The Pioneer", "Energetic, impulsive, action-oriented"),
            4: ("Wood", "The Charmer", "Romantic, creative, social"),
            5: ("Earth", "The Leader", "Powerful, commanding, transformative"),
            6: ("Metal", "The Mentor", "Responsible, perfectionist, idealistic"),
            7: ("Metal", "The Innovator", "Communicative, playful, pleasure-seeking"),
            8: ("Earth", "The Influencer", "Ambitious, determined, results-driven"),
            9: ("Fire", "The Inspiration", "Passionate, visionary, charismatic")
        }
        
        # Element interactions
        self.supportive_cycles = {
            "Water": ["Wood"],
            "Wood": ["Fire"],
            "Fire": ["Earth"],
            "Earth": ["Metal"],
            "Metal": ["Water"]
        }
        
        self.challenging_cycles = {
            "Water": ["Fire"],
            "Fire": ["Metal"],
            "Metal": ["Wood"],
            "Wood": ["Earth"],
            "Earth": ["Water"]
        }
        
    def calculate_main_number(self, birth_year: int, gender: str) -> int:
        """Calculate main energy number from birth year and gender"""
        # Sum digits of birth year
        year_sum = sum(int(digit) for digit in str(birth_year))
        # Reduce to single digit
        reduced = (year_sum - 1) % 9 + 1
        
        if gender.lower() == 'm':
            # Male calculation
            main_num = 11 - reduced
            if main_num <= 0:
                main_num += 9
        else:
            # Female calculation
            main_num = reduced + 4
            if main_num > 9:
                main_num -= 9
        
        return main_num
    
    def calculate_energy_number(self, birth_month: int, main_num: int) -> int:
        """Calculate energy number from birth month and main number"""
        # Simple calculation based on month and main number
        energy_num = (main_num + birth_month - 1) % 9
        return 9 if energy_num == 0 else energy_num
    
    def calculate_trend_number(self, birth_day: int, energy_num: int) -> int:
        """Calculate trend number from birth day and energy number"""
        trend_num = (energy_num + birth_day - 1) % 9
        return 9 if trend_num == 0 else trend_num
    
    def get_current_year_energy(self) -> int:
        """Calculate universal energy number for current Gregorian year"""
        current_year = datetime.datetime.now().year
        
        # The cycle pattern (based on Lo Shu square movement)
        # This is a simplified calculation - in practice it follows the Flying Star pattern
        cycle_pattern = [5, 6, 7, 8, 9, 1, 2, 3, 4]
        
        # Base year 2000 had energy 5
        base_year = 2000
        base_energy = 5
        
        # Calculate offset from base year
        years_diff = current_year - base_year
        energy_index = (years_diff % 9)
        
        return cycle_pattern[energy_index]
    
    def get_element_interaction(self, user_element: str, year_element: str) -> Tuple[str, str]:
        """Determine the interaction between user's element and year's element"""
        if user_element == year_element:
            return "Neutral", "You are in harmony with this year's energy. Focus on stability and consistency."
        
        if year_element in self.supportive_cycles.get(user_element, []):
            return "Supportive", "This year's energy supports you. A great time for growth and new beginnings."
        
        if user_element in self.supportive_cycles.get(year_element, []):
            return "Nourishing", "You nourish this year's energy. Focus on giving and contribution."
        
        if year_element in self.challenging_cycles.get(user_element, []):
            return "Challenging", "This year may bring challenges. Focus on patience and adaptability."
        
        if user_element in self.challenging_cycles.get(year_element, []):
            return "Controlling", "You control this year's energy. A good time for leadership and making changes."
        
        return "Neutral", "A balanced year. Focus on maintaining harmony."
    
    def get_forecast(self, user_num: int, year_num: int) -> str:
        """Generate forecast based on user's main number and year energy"""
        user_element = self.elements[user_num][0]
        year_element = self.elements[year_num][0]
        
        interaction, advice = self.get_element_interaction(user_element, year_element)
        
        forecast = f"""
        YEARLY FORECAST:
        Your Energy ({user_num} {user_element}) meets Year Energy ({year_num} {year_element})
        Interaction: {interaction}
        
        {advice}
        
        RECOMMENDED FOCUS:
        """
        
        # Add specific recommendations based on interaction
        if interaction == "Supportive":
            forecast += "- Take bold initiatives\n- Start new projects\n- Expand your social circle"
        elif interaction == "Challenging":
            forecast += "- Practice self-care\n- Be patient with obstacles\n- Focus on learning, not just outcomes"
        elif interaction == "Nourishing":
            forecast += "- Mentor others\n- Share your knowledge\n- Build supportive structures"
        elif interaction == "Controlling":
            forecast += "- Take leadership roles\n- Make strategic decisions\n- Implement changes"
        else:
            forecast += "- Maintain balance\n- Consolidate gains\n- Build strong foundations"
        
        return forecast

def main():
    print("=" * 50)
    print("      9 STAR KI ENERGY CALCULATOR")
    print("=" * 50)
    
    calculator = NineStarKiCalculator()
    
    # Get user input
    print("\nPlease enter your information:")
    
    birth_year = int(input("Birth Year (e.g., 1987): "))
    birth_month = int(input("Birth Month (1-12): "))
    birth_day = int(input("Birth Day (1-31): "))
    gender = input("Gender (M/F): ").strip().upper()
    
    # Calculate numbers
    main_num = calculator.calculate_main_number(birth_year, gender)
    energy_num = calculator.calculate_energy_number(birth_month, main_num)
    trend_num = calculator.calculate_trend_number(birth_day, energy_num)
    
    current_year = datetime.datetime.now().year
    year_energy = calculator.get_current_year_energy()
    
    # Display results
    print("\n" + "=" * 50)
    print("YOUR 9 STAR KI PROFILE")
    print("=" * 50)
    
    # Main number
    element, archetype, traits = calculator.elements[main_num]
    print(f"\nMAIN ENERGY ({main_num} - {element}):")
    print(f"Archetype: {archetype}")
    print(f"Traits: {traits}")
    
    # Energy number
    element_e, archetype_e, traits_e = calculator.elements[energy_num]
    print(f"\nHEART ENERGY ({energy_num} - {element_e}):")
    print(f"Your inner emotional world and motivations")
    
    # Trend number
    element_t, archetype_t, traits_t = calculator.elements[trend_num]
    print(f"\nTREND ENERGY ({trend_num} - {element_t}):")
    print(f"How you express yourself outwardly")
    
    print("\n" + "=" * 50)
    print(f"UNIVERSAL ENERGY FOR {current_year}")
    print("=" * 50)
    
    year_element, year_archetype, year_traits = calculator.elements[year_energy]
    print(f"\nCurrent Year Energy: {year_energy} - {year_element}")
    print(f"Theme: {year_archetype}")
    print(f"Global Influence: {year_traits}")
    
    # Get forecast
    forecast = calculator.get_forecast(main_num, year_energy)
    print("\n" + "=" * 50)
    print("PERSONAL YEARLY FORECAST")
    print("=" * 50)
    print(forecast)
    
    # Additional insights
    print("\n" + "=" * 50)
    print("QUICK INSIGHTS")
    print("=" * 50)
    
    # Lucky directions (simplified)
    directions = {
        1: "North",
        2: "Southwest",
        3: "East",
        4: "Southeast",
        5: "Center",
        6: "Northwest",
        7: "West",
        8: "Northeast",
        9: "South"
    }
    
    print(f"\nYour Auspicious Direction: {directions[main_num]}")
    
    # Element compatibility
    user_element = calculator.elements[main_num][0]
    compatible_elements = calculator.supportive_cycles.get(user_element, [])
    challenging_elements = calculator.challenging_cycles.get(user_element, [])
    
    print(f"\nYour element ({user_element}) works well with: {', '.join(compatible_elements) if compatible_elements else 'All elements in balance'}")
    print(f"Your element may face challenges with: {', '.join(challenging_elements) if challenging_elements else 'None - you harmonize well'}")
    
    print("\n" + "=" * 50)
    print("Remember: 9 Star Ki is a guide, not a destiny.")
    print("Use this energy awareness to make conscious choices.")
    print("=" * 50)

if __name__ == "__main__":
    main()