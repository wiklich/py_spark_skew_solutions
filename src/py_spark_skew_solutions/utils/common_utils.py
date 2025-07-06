"""
Commons methdos utilitaries.
"""


def validate_required_params(kwargs: dict, required_keys: list):
    """
    Validates that all required keys are present in the provided dictionary.

    Args:
        kwargs (dict): Dictionary containing parameters to be validated.
        required_keys (list of str): List of keys that must be present in kwargs.

    Raises:
        ValueError: If any key in required_keys is missing from kwargs.
    """
    missing_params = [key for key in required_keys if key not in kwargs or kwargs[key] is None]
    
    if missing_params:
        raise ValueError(f"Missing required parameters: {', '.join(repr(param) for param in missing_params)}")