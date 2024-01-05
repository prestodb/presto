**********************
Prestissimo Functions
**********************

Unicode Functions
------------------
.. function:: key_sampling_percent(varchar) -> double

    Generates a double value between 0.0 and 1.0 based on the hash of the given ``varchar``.
    This function is useful for deterministic sampling of data.