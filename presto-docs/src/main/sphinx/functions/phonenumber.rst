======================
Phone Number Functions
======================

Phone Number functions based on Google's `libphonenumber` library -
`https://github.com/googlei18n/libphonenumber`

.. function:: canonicalize_phone_number(phonenumber) -> varchar

    Canonicalize the phone number based on RFC3966 standard. The phone
    number should have the country code for this to work.

.. function:: canonicalize_phone_number(phonenumber, countrycode) -> varchar

    Canonicalize the phone number given the country based on RFC3966
    standard. The country code is represented using its ISO code.

.. function:: canonicalize_phone_number(phonenumber, countrycode, format) -> varchar

    Canonicalize the phone number given the country based on given
    standard. The possible formats are: `RFC3966`, `E164`, `NATIONAL`,
    `INTERNATIONAL`.
