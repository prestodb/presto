=================
Function Coverage
=================

Here is a list of all scalar and aggregate Presto functions with functions that are available in Velox highlighted.

.. raw:: html

    <style>
    div.body {max-width: 1300px;}
    table.coverage th {background-color: lightblue; text-align: center;}
    table.coverage td:nth-child(6) {background-color: lightblue;}
    table.coverage td:nth-child(8) {background-color: lightblue;}
    table.coverage tr:nth-child(1) td:nth-child(1) {background-color: #6BA81E;}
    table.coverage tr:nth-child(1) td:nth-child(2) {background-color: #6BA81E;}
    table.coverage tr:nth-child(1) td:nth-child(3) {background-color: #6BA81E;}
    table.coverage tr:nth-child(1) td:nth-child(4) {background-color: #6BA81E;}
    table.coverage tr:nth-child(1) td:nth-child(7) {background-color: #6BA81E;}
    table.coverage tr:nth-child(1) td:nth-child(9) {background-color: #6BA81E;}
    table.coverage tr:nth-child(2) td:nth-child(1) {background-color: #6BA81E;}
    table.coverage tr:nth-child(2) td:nth-child(2) {background-color: #6BA81E;}
    table.coverage tr:nth-child(2) td:nth-child(3) {background-color: #6BA81E;}
    table.coverage tr:nth-child(2) td:nth-child(4) {background-color: #6BA81E;}
    table.coverage tr:nth-child(2) td:nth-child(7) {background-color: #6BA81E;}
    table.coverage tr:nth-child(2) td:nth-child(9) {background-color: #6BA81E;}
    table.coverage tr:nth-child(3) td:nth-child(1) {background-color: #6BA81E;}
    table.coverage tr:nth-child(3) td:nth-child(2) {background-color: #6BA81E;}
    table.coverage tr:nth-child(3) td:nth-child(3) {background-color: #6BA81E;}
    table.coverage tr:nth-child(3) td:nth-child(4) {background-color: #6BA81E;}
    table.coverage tr:nth-child(3) td:nth-child(7) {background-color: #6BA81E;}
    table.coverage tr:nth-child(3) td:nth-child(9) {background-color: #6BA81E;}
    table.coverage tr:nth-child(4) td:nth-child(1) {background-color: #6BA81E;}
    table.coverage tr:nth-child(4) td:nth-child(2) {background-color: #6BA81E;}
    table.coverage tr:nth-child(4) td:nth-child(3) {background-color: #6BA81E;}
    table.coverage tr:nth-child(4) td:nth-child(4) {background-color: #6BA81E;}
    table.coverage tr:nth-child(4) td:nth-child(7) {background-color: #6BA81E;}
    table.coverage tr:nth-child(4) td:nth-child(9) {background-color: #6BA81E;}
    table.coverage tr:nth-child(5) td:nth-child(1) {background-color: #6BA81E;}
    table.coverage tr:nth-child(5) td:nth-child(2) {background-color: #6BA81E;}
    table.coverage tr:nth-child(5) td:nth-child(7) {background-color: #6BA81E;}
    table.coverage tr:nth-child(5) td:nth-child(9) {background-color: #6BA81E;}
    table.coverage tr:nth-child(6) td:nth-child(1) {background-color: #6BA81E;}
    table.coverage tr:nth-child(6) td:nth-child(2) {background-color: #6BA81E;}
    table.coverage tr:nth-child(6) td:nth-child(7) {background-color: #6BA81E;}
    table.coverage tr:nth-child(6) td:nth-child(9) {background-color: #6BA81E;}
    table.coverage tr:nth-child(7) td:nth-child(1) {background-color: #6BA81E;}
    table.coverage tr:nth-child(7) td:nth-child(2) {background-color: #6BA81E;}
    table.coverage tr:nth-child(7) td:nth-child(3) {background-color: #6BA81E;}
    table.coverage tr:nth-child(7) td:nth-child(7) {background-color: #6BA81E;}
    table.coverage tr:nth-child(7) td:nth-child(9) {background-color: #6BA81E;}
    table.coverage tr:nth-child(8) td:nth-child(2) {background-color: #6BA81E;}
    table.coverage tr:nth-child(8) td:nth-child(4) {background-color: #6BA81E;}
    table.coverage tr:nth-child(8) td:nth-child(7) {background-color: #6BA81E;}
    table.coverage tr:nth-child(8) td:nth-child(9) {background-color: #6BA81E;}
    table.coverage tr:nth-child(9) td:nth-child(1) {background-color: #6BA81E;}
    table.coverage tr:nth-child(9) td:nth-child(2) {background-color: #6BA81E;}
    table.coverage tr:nth-child(9) td:nth-child(3) {background-color: #6BA81E;}
    table.coverage tr:nth-child(9) td:nth-child(7) {background-color: #6BA81E;}
    table.coverage tr:nth-child(9) td:nth-child(9) {background-color: #6BA81E;}
    table.coverage tr:nth-child(10) td:nth-child(1) {background-color: #6BA81E;}
    table.coverage tr:nth-child(10) td:nth-child(2) {background-color: #6BA81E;}
    table.coverage tr:nth-child(10) td:nth-child(3) {background-color: #6BA81E;}
    table.coverage tr:nth-child(10) td:nth-child(4) {background-color: #6BA81E;}
    table.coverage tr:nth-child(10) td:nth-child(7) {background-color: #6BA81E;}
    table.coverage tr:nth-child(10) td:nth-child(9) {background-color: #6BA81E;}
    table.coverage tr:nth-child(11) td:nth-child(1) {background-color: #6BA81E;}
    table.coverage tr:nth-child(11) td:nth-child(2) {background-color: #6BA81E;}
    table.coverage tr:nth-child(11) td:nth-child(3) {background-color: #6BA81E;}
    table.coverage tr:nth-child(11) td:nth-child(4) {background-color: #6BA81E;}
    table.coverage tr:nth-child(11) td:nth-child(7) {background-color: #6BA81E;}
    table.coverage tr:nth-child(11) td:nth-child(9) {background-color: #6BA81E;}
    table.coverage tr:nth-child(12) td:nth-child(1) {background-color: #6BA81E;}
    table.coverage tr:nth-child(12) td:nth-child(2) {background-color: #6BA81E;}
    table.coverage tr:nth-child(12) td:nth-child(3) {background-color: #6BA81E;}
    table.coverage tr:nth-child(12) td:nth-child(4) {background-color: #6BA81E;}
    table.coverage tr:nth-child(12) td:nth-child(7) {background-color: #6BA81E;}
    table.coverage tr:nth-child(13) td:nth-child(1) {background-color: #6BA81E;}
    table.coverage tr:nth-child(13) td:nth-child(2) {background-color: #6BA81E;}
    table.coverage tr:nth-child(13) td:nth-child(3) {background-color: #6BA81E;}
    table.coverage tr:nth-child(14) td:nth-child(1) {background-color: #6BA81E;}
    table.coverage tr:nth-child(14) td:nth-child(2) {background-color: #6BA81E;}
    table.coverage tr:nth-child(14) td:nth-child(3) {background-color: #6BA81E;}
    table.coverage tr:nth-child(14) td:nth-child(4) {background-color: #6BA81E;}
    table.coverage tr:nth-child(15) td:nth-child(1) {background-color: #6BA81E;}
    table.coverage tr:nth-child(15) td:nth-child(4) {background-color: #6BA81E;}
    table.coverage tr:nth-child(16) td:nth-child(1) {background-color: #6BA81E;}
    table.coverage tr:nth-child(16) td:nth-child(2) {background-color: #6BA81E;}
    table.coverage tr:nth-child(16) td:nth-child(3) {background-color: #6BA81E;}
    table.coverage tr:nth-child(16) td:nth-child(4) {background-color: #6BA81E;}
    table.coverage tr:nth-child(17) td:nth-child(3) {background-color: #6BA81E;}
    table.coverage tr:nth-child(17) td:nth-child(5) {background-color: #6BA81E;}
    table.coverage tr:nth-child(18) td:nth-child(1) {background-color: #6BA81E;}
    table.coverage tr:nth-child(18) td:nth-child(2) {background-color: #6BA81E;}
    table.coverage tr:nth-child(18) td:nth-child(3) {background-color: #6BA81E;}
    table.coverage tr:nth-child(18) td:nth-child(4) {background-color: #6BA81E;}
    table.coverage tr:nth-child(18) td:nth-child(5) {background-color: #6BA81E;}
    table.coverage tr:nth-child(19) td:nth-child(3) {background-color: #6BA81E;}
    table.coverage tr:nth-child(19) td:nth-child(5) {background-color: #6BA81E;}
    table.coverage tr:nth-child(19) td:nth-child(7) {background-color: #6BA81E;}
    table.coverage tr:nth-child(20) td:nth-child(1) {background-color: #6BA81E;}
    table.coverage tr:nth-child(20) td:nth-child(2) {background-color: #6BA81E;}
    table.coverage tr:nth-child(20) td:nth-child(3) {background-color: #6BA81E;}
    table.coverage tr:nth-child(20) td:nth-child(4) {background-color: #6BA81E;}
    table.coverage tr:nth-child(20) td:nth-child(5) {background-color: #6BA81E;}
    table.coverage tr:nth-child(20) td:nth-child(7) {background-color: #6BA81E;}
    table.coverage tr:nth-child(21) td:nth-child(1) {background-color: #6BA81E;}
    table.coverage tr:nth-child(21) td:nth-child(2) {background-color: #6BA81E;}
    table.coverage tr:nth-child(21) td:nth-child(4) {background-color: #6BA81E;}
    table.coverage tr:nth-child(21) td:nth-child(5) {background-color: #6BA81E;}
    table.coverage tr:nth-child(21) td:nth-child(7) {background-color: #6BA81E;}
    table.coverage tr:nth-child(22) td:nth-child(1) {background-color: #6BA81E;}
    table.coverage tr:nth-child(22) td:nth-child(2) {background-color: #6BA81E;}
    table.coverage tr:nth-child(22) td:nth-child(4) {background-color: #6BA81E;}
    table.coverage tr:nth-child(22) td:nth-child(5) {background-color: #6BA81E;}
    table.coverage tr:nth-child(22) td:nth-child(7) {background-color: #6BA81E;}
    table.coverage tr:nth-child(23) td:nth-child(1) {background-color: #6BA81E;}
    table.coverage tr:nth-child(23) td:nth-child(2) {background-color: #6BA81E;}
    table.coverage tr:nth-child(23) td:nth-child(3) {background-color: #6BA81E;}
    table.coverage tr:nth-child(23) td:nth-child(4) {background-color: #6BA81E;}
    table.coverage tr:nth-child(23) td:nth-child(7) {background-color: #6BA81E;}
    table.coverage tr:nth-child(24) td:nth-child(1) {background-color: #6BA81E;}
    table.coverage tr:nth-child(24) td:nth-child(2) {background-color: #6BA81E;}
    table.coverage tr:nth-child(24) td:nth-child(4) {background-color: #6BA81E;}
    table.coverage tr:nth-child(24) td:nth-child(5) {background-color: #6BA81E;}
    table.coverage tr:nth-child(25) td:nth-child(1) {background-color: #6BA81E;}
    table.coverage tr:nth-child(25) td:nth-child(4) {background-color: #6BA81E;}
    table.coverage tr:nth-child(25) td:nth-child(5) {background-color: #6BA81E;}
    table.coverage tr:nth-child(25) td:nth-child(7) {background-color: #6BA81E;}
    table.coverage tr:nth-child(26) td:nth-child(1) {background-color: #6BA81E;}
    table.coverage tr:nth-child(26) td:nth-child(2) {background-color: #6BA81E;}
    table.coverage tr:nth-child(26) td:nth-child(3) {background-color: #6BA81E;}
    table.coverage tr:nth-child(26) td:nth-child(5) {background-color: #6BA81E;}
    table.coverage tr:nth-child(27) td:nth-child(1) {background-color: #6BA81E;}
    table.coverage tr:nth-child(27) td:nth-child(3) {background-color: #6BA81E;}
    table.coverage tr:nth-child(27) td:nth-child(4) {background-color: #6BA81E;}
    table.coverage tr:nth-child(27) td:nth-child(5) {background-color: #6BA81E;}
    table.coverage tr:nth-child(27) td:nth-child(7) {background-color: #6BA81E;}
    table.coverage tr:nth-child(28) td:nth-child(1) {background-color: #6BA81E;}
    table.coverage tr:nth-child(28) td:nth-child(3) {background-color: #6BA81E;}
    table.coverage tr:nth-child(28) td:nth-child(4) {background-color: #6BA81E;}
    table.coverage tr:nth-child(28) td:nth-child(5) {background-color: #6BA81E;}
    table.coverage tr:nth-child(28) td:nth-child(7) {background-color: #6BA81E;}
    table.coverage tr:nth-child(29) td:nth-child(1) {background-color: #6BA81E;}
    table.coverage tr:nth-child(29) td:nth-child(3) {background-color: #6BA81E;}
    table.coverage tr:nth-child(29) td:nth-child(5) {background-color: #6BA81E;}
    table.coverage tr:nth-child(30) td:nth-child(1) {background-color: #6BA81E;}
    table.coverage tr:nth-child(30) td:nth-child(3) {background-color: #6BA81E;}
    table.coverage tr:nth-child(30) td:nth-child(4) {background-color: #6BA81E;}
    table.coverage tr:nth-child(30) td:nth-child(5) {background-color: #6BA81E;}
    table.coverage tr:nth-child(30) td:nth-child(7) {background-color: #6BA81E;}
    table.coverage tr:nth-child(31) td:nth-child(2) {background-color: #6BA81E;}
    table.coverage tr:nth-child(31) td:nth-child(3) {background-color: #6BA81E;}
    table.coverage tr:nth-child(31) td:nth-child(4) {background-color: #6BA81E;}
    table.coverage tr:nth-child(32) td:nth-child(1) {background-color: #6BA81E;}
    table.coverage tr:nth-child(32) td:nth-child(2) {background-color: #6BA81E;}
    table.coverage tr:nth-child(32) td:nth-child(3) {background-color: #6BA81E;}
    table.coverage tr:nth-child(32) td:nth-child(4) {background-color: #6BA81E;}
    table.coverage tr:nth-child(32) td:nth-child(5) {background-color: #6BA81E;}
    table.coverage tr:nth-child(32) td:nth-child(7) {background-color: #6BA81E;}
    table.coverage tr:nth-child(33) td:nth-child(3) {background-color: #6BA81E;}
    table.coverage tr:nth-child(33) td:nth-child(5) {background-color: #6BA81E;}
    table.coverage tr:nth-child(34) td:nth-child(2) {background-color: #6BA81E;}
    table.coverage tr:nth-child(34) td:nth-child(3) {background-color: #6BA81E;}
    table.coverage tr:nth-child(34) td:nth-child(4) {background-color: #6BA81E;}
    table.coverage tr:nth-child(34) td:nth-child(5) {background-color: #6BA81E;}
    table.coverage tr:nth-child(35) td:nth-child(2) {background-color: #6BA81E;}
    table.coverage tr:nth-child(35) td:nth-child(3) {background-color: #6BA81E;}
    table.coverage tr:nth-child(35) td:nth-child(4) {background-color: #6BA81E;}
    table.coverage tr:nth-child(36) td:nth-child(2) {background-color: #6BA81E;}
    table.coverage tr:nth-child(36) td:nth-child(3) {background-color: #6BA81E;}
    table.coverage tr:nth-child(36) td:nth-child(4) {background-color: #6BA81E;}
    table.coverage tr:nth-child(37) td:nth-child(2) {background-color: #6BA81E;}
    table.coverage tr:nth-child(37) td:nth-child(3) {background-color: #6BA81E;}
    table.coverage tr:nth-child(38) td:nth-child(2) {background-color: #6BA81E;}
    table.coverage tr:nth-child(38) td:nth-child(5) {background-color: #6BA81E;}
    table.coverage tr:nth-child(38) td:nth-child(7) {background-color: #6BA81E;}
    table.coverage tr:nth-child(39) td:nth-child(3) {background-color: #6BA81E;}
    table.coverage tr:nth-child(39) td:nth-child(5) {background-color: #6BA81E;}
    table.coverage tr:nth-child(39) td:nth-child(7) {background-color: #6BA81E;}
    table.coverage tr:nth-child(40) td:nth-child(2) {background-color: #6BA81E;}
    table.coverage tr:nth-child(40) td:nth-child(3) {background-color: #6BA81E;}
    table.coverage tr:nth-child(40) td:nth-child(5) {background-color: #6BA81E;}
    table.coverage tr:nth-child(40) td:nth-child(7) {background-color: #6BA81E;}
    table.coverage tr:nth-child(41) td:nth-child(2) {background-color: #6BA81E;}
    table.coverage tr:nth-child(41) td:nth-child(5) {background-color: #6BA81E;}
    table.coverage tr:nth-child(41) td:nth-child(7) {background-color: #6BA81E;}
    table.coverage tr:nth-child(42) td:nth-child(1) {background-color: #6BA81E;}
    table.coverage tr:nth-child(42) td:nth-child(5) {background-color: #6BA81E;}
    table.coverage tr:nth-child(42) td:nth-child(7) {background-color: #6BA81E;}
    table.coverage tr:nth-child(43) td:nth-child(1) {background-color: #6BA81E;}
    table.coverage tr:nth-child(43) td:nth-child(2) {background-color: #6BA81E;}
    table.coverage tr:nth-child(43) td:nth-child(3) {background-color: #6BA81E;}
    table.coverage tr:nth-child(43) td:nth-child(5) {background-color: #6BA81E;}
    table.coverage tr:nth-child(43) td:nth-child(7) {background-color: #6BA81E;}
    table.coverage tr:nth-child(44) td:nth-child(1) {background-color: #6BA81E;}
    table.coverage tr:nth-child(44) td:nth-child(2) {background-color: #6BA81E;}
    table.coverage tr:nth-child(44) td:nth-child(3) {background-color: #6BA81E;}
    table.coverage tr:nth-child(44) td:nth-child(5) {background-color: #6BA81E;}
    table.coverage tr:nth-child(45) td:nth-child(1) {background-color: #6BA81E;}
    table.coverage tr:nth-child(45) td:nth-child(2) {background-color: #6BA81E;}
    table.coverage tr:nth-child(45) td:nth-child(3) {background-color: #6BA81E;}
    table.coverage tr:nth-child(45) td:nth-child(5) {background-color: #6BA81E;}
    table.coverage tr:nth-child(45) td:nth-child(7) {background-color: #6BA81E;}
    table.coverage tr:nth-child(46) td:nth-child(1) {background-color: #6BA81E;}
    table.coverage tr:nth-child(46) td:nth-child(5) {background-color: #6BA81E;}
    table.coverage tr:nth-child(46) td:nth-child(7) {background-color: #6BA81E;}
    table.coverage tr:nth-child(47) td:nth-child(1) {background-color: #6BA81E;}
    table.coverage tr:nth-child(47) td:nth-child(7) {background-color: #6BA81E;}
    table.coverage tr:nth-child(48) td:nth-child(1) {background-color: #6BA81E;}
    table.coverage tr:nth-child(48) td:nth-child(3) {background-color: #6BA81E;}
    table.coverage tr:nth-child(48) td:nth-child(5) {background-color: #6BA81E;}
    table.coverage tr:nth-child(49) td:nth-child(1) {background-color: #6BA81E;}
    table.coverage tr:nth-child(49) td:nth-child(3) {background-color: #6BA81E;}
    table.coverage tr:nth-child(49) td:nth-child(5) {background-color: #6BA81E;}
    table.coverage tr:nth-child(50) td:nth-child(1) {background-color: #6BA81E;}
    table.coverage tr:nth-child(50) td:nth-child(3) {background-color: #6BA81E;}
    table.coverage tr:nth-child(50) td:nth-child(5) {background-color: #6BA81E;}
    table.coverage tr:nth-child(50) td:nth-child(7) {background-color: #6BA81E;}
    table.coverage tr:nth-child(51) td:nth-child(1) {background-color: #6BA81E;}
    table.coverage tr:nth-child(51) td:nth-child(3) {background-color: #6BA81E;}
    table.coverage tr:nth-child(51) td:nth-child(5) {background-color: #6BA81E;}
    table.coverage tr:nth-child(51) td:nth-child(7) {background-color: #6BA81E;}
    table.coverage tr:nth-child(52) td:nth-child(1) {background-color: #6BA81E;}
    table.coverage tr:nth-child(52) td:nth-child(3) {background-color: #6BA81E;}
    table.coverage tr:nth-child(52) td:nth-child(5) {background-color: #6BA81E;}
    table.coverage tr:nth-child(52) td:nth-child(7) {background-color: #6BA81E;}
    table.coverage tr:nth-child(53) td:nth-child(1) {background-color: #6BA81E;}
    table.coverage tr:nth-child(53) td:nth-child(5) {background-color: #6BA81E;}
    table.coverage tr:nth-child(53) td:nth-child(7) {background-color: #6BA81E;}
    table.coverage tr:nth-child(54) td:nth-child(1) {background-color: #6BA81E;}
    table.coverage tr:nth-child(54) td:nth-child(2) {background-color: #6BA81E;}
    table.coverage tr:nth-child(54) td:nth-child(5) {background-color: #6BA81E;}
    table.coverage tr:nth-child(54) td:nth-child(7) {background-color: #6BA81E;}
    table.coverage tr:nth-child(55) td:nth-child(1) {background-color: #6BA81E;}
    table.coverage tr:nth-child(55) td:nth-child(5) {background-color: #6BA81E;}
    table.coverage tr:nth-child(55) td:nth-child(7) {background-color: #6BA81E;}
    table.coverage tr:nth-child(56) td:nth-child(1) {background-color: #6BA81E;}
    table.coverage tr:nth-child(56) td:nth-child(3) {background-color: #6BA81E;}
    table.coverage tr:nth-child(56) td:nth-child(5) {background-color: #6BA81E;}
    table.coverage tr:nth-child(56) td:nth-child(7) {background-color: #6BA81E;}
    table.coverage tr:nth-child(57) td:nth-child(1) {background-color: #6BA81E;}
    table.coverage tr:nth-child(57) td:nth-child(2) {background-color: #6BA81E;}
    table.coverage tr:nth-child(57) td:nth-child(3) {background-color: #6BA81E;}
    table.coverage tr:nth-child(57) td:nth-child(5) {background-color: #6BA81E;}
    table.coverage tr:nth-child(57) td:nth-child(7) {background-color: #6BA81E;}
    table.coverage tr:nth-child(58) td:nth-child(1) {background-color: #6BA81E;}
    table.coverage tr:nth-child(58) td:nth-child(2) {background-color: #6BA81E;}
    table.coverage tr:nth-child(58) td:nth-child(3) {background-color: #6BA81E;}
    table.coverage tr:nth-child(58) td:nth-child(7) {background-color: #6BA81E;}
    table.coverage tr:nth-child(59) td:nth-child(1) {background-color: #6BA81E;}
    table.coverage tr:nth-child(59) td:nth-child(2) {background-color: #6BA81E;}
    table.coverage tr:nth-child(59) td:nth-child(3) {background-color: #6BA81E;}
    table.coverage tr:nth-child(59) td:nth-child(7) {background-color: #6BA81E;}
    table.coverage tr:nth-child(60) td:nth-child(1) {background-color: #6BA81E;}
    table.coverage tr:nth-child(60) td:nth-child(2) {background-color: #6BA81E;}
    table.coverage tr:nth-child(60) td:nth-child(3) {background-color: #6BA81E;}
    table.coverage tr:nth-child(60) td:nth-child(7) {background-color: #6BA81E;}
    table.coverage tr:nth-child(61) td:nth-child(2) {background-color: #6BA81E;}
    table.coverage tr:nth-child(61) td:nth-child(3) {background-color: #6BA81E;}
    table.coverage tr:nth-child(61) td:nth-child(5) {background-color: #6BA81E;}
    table.coverage tr:nth-child(61) td:nth-child(7) {background-color: #6BA81E;}
    table.coverage tr:nth-child(62) td:nth-child(1) {background-color: #6BA81E;}
    table.coverage tr:nth-child(62) td:nth-child(2) {background-color: #6BA81E;}
    table.coverage tr:nth-child(62) td:nth-child(5) {background-color: #6BA81E;}
    table.coverage tr:nth-child(62) td:nth-child(7) {background-color: #6BA81E;}
    table.coverage tr:nth-child(63) td:nth-child(5) {background-color: #6BA81E;}
    table.coverage tr:nth-child(64) td:nth-child(1) {background-color: #6BA81E;}
    table.coverage tr:nth-child(64) td:nth-child(2) {background-color: #6BA81E;}
    table.coverage tr:nth-child(64) td:nth-child(3) {background-color: #6BA81E;}
    table.coverage tr:nth-child(64) td:nth-child(5) {background-color: #6BA81E;}
    table.coverage tr:nth-child(64) td:nth-child(7) {background-color: #6BA81E;}
    table.coverage tr:nth-child(65) td:nth-child(1) {background-color: #6BA81E;}
    table.coverage tr:nth-child(65) td:nth-child(5) {background-color: #6BA81E;}
    table.coverage tr:nth-child(65) td:nth-child(7) {background-color: #6BA81E;}
    table.coverage tr:nth-child(66) td:nth-child(1) {background-color: #6BA81E;}
    table.coverage tr:nth-child(66) td:nth-child(5) {background-color: #6BA81E;}
    table.coverage tr:nth-child(66) td:nth-child(7) {background-color: #6BA81E;}
    table.coverage tr:nth-child(67) td:nth-child(1) {background-color: #6BA81E;}
    table.coverage tr:nth-child(67) td:nth-child(3) {background-color: #6BA81E;}
    table.coverage tr:nth-child(67) td:nth-child(7) {background-color: #6BA81E;}
    table.coverage tr:nth-child(68) td:nth-child(1) {background-color: #6BA81E;}
    table.coverage tr:nth-child(68) td:nth-child(5) {background-color: #6BA81E;}
    table.coverage tr:nth-child(69) td:nth-child(1) {background-color: #6BA81E;}
    table.coverage tr:nth-child(69) td:nth-child(3) {background-color: #6BA81E;}
    table.coverage tr:nth-child(69) td:nth-child(5) {background-color: #6BA81E;}
    table.coverage tr:nth-child(69) td:nth-child(7) {background-color: #6BA81E;}
    table.coverage tr:nth-child(70) td:nth-child(1) {background-color: #6BA81E;}
    table.coverage tr:nth-child(70) td:nth-child(3) {background-color: #6BA81E;}
    table.coverage tr:nth-child(70) td:nth-child(5) {background-color: #6BA81E;}
    table.coverage tr:nth-child(70) td:nth-child(7) {background-color: #6BA81E;}
    table.coverage tr:nth-child(71) td:nth-child(1) {background-color: #6BA81E;}
    table.coverage tr:nth-child(71) td:nth-child(3) {background-color: #6BA81E;}
    table.coverage tr:nth-child(71) td:nth-child(5) {background-color: #6BA81E;}
    table.coverage tr:nth-child(71) td:nth-child(7) {background-color: #6BA81E;}
    table.coverage tr:nth-child(72) td:nth-child(5) {background-color: #6BA81E;}
    table.coverage tr:nth-child(73) td:nth-child(3) {background-color: #6BA81E;}
    table.coverage tr:nth-child(73) td:nth-child(5) {background-color: #6BA81E;}
    table.coverage tr:nth-child(74) td:nth-child(3) {background-color: #6BA81E;}
    table.coverage tr:nth-child(75) td:nth-child(1) {background-color: #6BA81E;}
    table.coverage tr:nth-child(75) td:nth-child(3) {background-color: #6BA81E;}
    table.coverage tr:nth-child(76) td:nth-child(1) {background-color: #6BA81E;}
    table.coverage tr:nth-child(76) td:nth-child(3) {background-color: #6BA81E;}
    table.coverage tr:nth-child(77) td:nth-child(1) {background-color: #6BA81E;}
    table.coverage tr:nth-child(77) td:nth-child(3) {background-color: #6BA81E;}
    </style>

.. table::
    :widths: auto
    :class: coverage

    ========================================  ========================================  ========================================  ========================================  ========================================  ==  ========================================  ==  ========================================
    Scalar Functions                                                                                                                                                                                                      Aggregate Functions                           Window Functions
    ================================================================================================================================================================================================================  ==  ========================================  ==  ========================================
    :func:`abs`                               :func:`date_format`                       :func:`is_finite`                         :func:`regexp_extract`                    st_point                                      :func:`approx_distinct`                       :func:`cume_dist`
    :func:`acos`                              :func:`date_parse`                        :func:`is_infinite`                       :func:`regexp_extract_all`                st_pointn                                     :func:`approx_most_frequent`                  :func:`dense_rank`
    :func:`all_match`                         :func:`date_trunc`                        :func:`is_json_scalar`                    :func:`regexp_like`                       st_points                                     :func:`approx_percentile`                     :func:`first_value`
    :func:`any_keys_match`                    :func:`day`                               :func:`is_nan`                            :func:`regexp_replace`                    st_polygon                                    :func:`approx_set`                            :func:`lag`
    :func:`any_match`                         :func:`day_of_month`                      is_subnet_of                              regexp_split                              st_relate                                     :func:`arbitrary`                             :func:`last_value`
    :func:`any_values_match`                  :func:`day_of_week`                       jaccard_index                             regress                                   st_startpoint                                 :func:`array_agg`                             :func:`lead`
    :func:`array_average`                     :func:`day_of_year`                       :func:`json_array_contains`               reidentification_potential                st_symdifference                              :func:`avg`                                   :func:`nth_value`
    array_cum_sum                             :func:`degrees`                           json_array_get                            :func:`remove_nulls`                      st_touches                                    :func:`bitwise_and_agg`                       :func:`ntile`
    :func:`array_distinct`                    :func:`dow`                               :func:`json_array_length`                 render                                    st_union                                      :func:`bitwise_or_agg`                        :func:`percent_rank`
    :func:`array_duplicates`                  :func:`doy`                               :func:`json_extract`                      :func:`repeat`                            st_within                                     :func:`bool_and`                              :func:`rank`
    :func:`array_except`                      :func:`e`                                 :func:`json_extract_scalar`               :func:`replace`                           st_x                                          :func:`bool_or`                               :func:`row_number`
    :func:`array_frequency`                   :func:`element_at`                        :func:`json_format`                       :func:`reverse`                           st_xmax                                       :func:`checksum`
    :func:`array_has_duplicates`              :func:`empty_approx_set`                  :func:`json_parse`                        rgb                                       st_xmin                                       classification_fall_out
    :func:`array_intersect`                   :func:`ends_with`                         :func:`json_size`                         :func:`round`                             st_y                                          classification_miss_rate
    :func:`array_join`                        enum_key                                  key_sampling_percent                      :func:`rpad`                              st_ymax                                       classification_precision
    :func:`array_max`                         :func:`exp`                               :func:`laplace_cdf`                       :func:`rtrim`                             st_ymin                                       classification_recall
    array_max_by                              expand_envelope                           :func:`last_day_of_month`                 scale_qdigest                             :func:`starts_with`                           classification_thresholds
    :func:`array_min`                         :func:`f_cdf`                             :func:`least`                             :func:`second`                            :func:`strpos`                                convex_hull_agg
    array_min_by                              features                                  :func:`length`                            secure_random                             :func:`strrpos`                               :func:`corr`
    :func:`array_normalize`                   :func:`filter`                            :func:`levenshtein_distance`              :func:`sequence`                          :func:`substr`                                :func:`count`
    :func:`array_position`                    :func:`filter`                            line_interpolate_point                    :func:`sha1`                              :func:`tan`                                   :func:`count_if`
    :func:`array_remove`                      :func:`find_first`                        line_locate_point                         :func:`sha256`                            :func:`tanh`                                  :func:`covar_pop`
    :func:`array_sort`                        :func:`find_first_index`                  :func:`ln`                                :func:`sha512`                            tdigest_agg                                   :func:`covar_samp`
    :func:`array_sort_desc`                   :func:`flatten`                           localtime                                 :func:`shuffle`                           :func:`timezone_hour`                         differential_entropy
    :func:`array_sum`                         flatten_geometry_collections              localtimestamp                            :func:`sign`                              :func:`timezone_minute`                       :func:`entropy`
    :func:`array_union`                       :func:`floor`                             :func:`log10`                             simplify_geometry                         :func:`to_base`                               evaluate_classifier_predictions
    :func:`arrays_overlap`                    fnv1_32                                   :func:`log2`                              :func:`sin`                               :func:`to_base64`                             :func:`every`
    :func:`asin`                              fnv1_64                                   :func:`lower`                             :func:`slice`                             :func:`to_base64url`                          :func:`geometric_mean`
    :func:`atan`                              fnv1a_32                                  :func:`lpad`                              spatial_partitions                        :func:`to_big_endian_32`                      geometry_union_agg
    :func:`atan2`                             fnv1a_64                                  :func:`ltrim`                             :func:`split`                             :func:`to_big_endian_64`                      :func:`histogram`
    bar                                       :func:`format_datetime`                   :func:`map`                               :func:`split_part`                        to_geometry                                   khyperloglog_agg
    :func:`beta_cdf`                          :func:`from_base`                         :func:`map_concat`                        :func:`split_to_map`                      :func:`to_hex`                                :func:`kurtosis`
    bing_tile                                 from_base32                               :func:`map_entries`                       split_to_multimap                         :func:`to_ieee754_32`                         learn_classifier
    bing_tile_at                              :func:`from_base64`                       :func:`map_filter`                        :func:`spooky_hash_v2_32`                 :func:`to_ieee754_64`                         learn_libsvm_classifier
    bing_tile_children                        :func:`from_base64url`                    :func:`map_from_entries`                  :func:`spooky_hash_v2_64`                 to_iso8601                                    learn_libsvm_regressor
    bing_tile_coordinates                     :func:`from_big_endian_32`                :func:`map_keys`                          :func:`sqrt`                              to_milliseconds                               learn_regressor
    bing_tile_parent                          :func:`from_big_endian_64`                :func:`map_normalize`                     st_area                                   to_spherical_geography                        make_set_digest
    bing_tile_polygon                         :func:`from_hex`                          map_remove_null_values                    st_asbinary                               :func:`to_unixtime`                           :func:`map_agg`
    bing_tile_quadkey                         from_ieee754_32                           :func:`map_subset`                        st_astext                                 :func:`to_utf8`                               :func:`map_union`
    bing_tile_zoom_level                      :func:`from_ieee754_64`                   :func:`map_top_n`                         st_boundary                               :func:`transform`                             :func:`map_union_sum`
    bing_tiles_around                         :func:`from_iso8601_date`                 map_top_n_keys                            st_buffer                                 :func:`transform_keys`                        :func:`max`
    :func:`binomial_cdf`                      from_iso8601_timestamp                    map_top_n_values                          st_centroid                               :func:`transform_values`                      :func:`max_by`
    :func:`bit_count`                         :func:`from_unixtime`                     :func:`map_values`                        st_contains                               :func:`trim`                                  :func:`merge`
    :func:`bitwise_and`                       :func:`from_utf8`                         :func:`map_zip_with`                      st_convexhull                             :func:`trim_array`                            merge_set_digest
    :func:`bitwise_arithmetic_shift_right`    :func:`gamma_cdf`                         :func:`md5`                               st_coorddim                               :func:`truncate`                              :func:`min`
    :func:`bitwise_left_shift`                geometry_as_geojson                       merge_hll                                 st_crosses                                :func:`typeof`                                :func:`min_by`
    :func:`bitwise_logical_shift_right`       geometry_from_geojson                     merge_khll                                st_difference                             uniqueness_distribution                       :func:`multimap_agg`
    :func:`bitwise_not`                       geometry_invalid_reason                   :func:`millisecond`                       st_dimension                              :func:`upper`                                 numeric_histogram
    :func:`bitwise_or`                        geometry_nearest_points                   :func:`minute`                            st_disjoint                               :func:`url_decode`                            qdigest_agg
    :func:`bitwise_right_shift`               geometry_to_bing_tiles                    :func:`mod`                               st_distance                               :func:`url_encode`                            :func:`reduce_agg`
    :func:`bitwise_right_shift_arithmetic`    geometry_to_dissolved_bing_tiles          :func:`month`                             st_endpoint                               :func:`url_extract_fragment`                  :func:`regr_avgx`
    :func:`bitwise_shift_left`                geometry_union                            :func:`multimap_from_entries`             st_envelope                               :func:`url_extract_host`                      :func:`regr_avgy`
    :func:`bitwise_xor`                       great_circle_distance                     murmur3_x64_128                           st_envelopeaspts                          :func:`url_extract_parameter`                 :func:`regr_count`
    :func:`cardinality`                       :func:`greatest`                          myanmar_font_encoding                     st_equals                                 :func:`url_extract_path`                      :func:`regr_intercept`
    :func:`cauchy_cdf`                        hamming_distance                          myanmar_normalize_unicode                 st_exteriorring                           :func:`url_extract_port`                      :func:`regr_r2`
    :func:`cbrt`                              hash_counts                               :func:`nan`                               st_geometries                             :func:`url_extract_protocol`                  :func:`regr_slope`
    :func:`ceil`                              :func:`hmac_md5`                          :func:`ngrams`                            st_geometryfromtext                       :func:`url_extract_query`                     :func:`regr_sxx`
    :func:`ceiling`                           :func:`hmac_sha1`                         :func:`no_keys_match`                     st_geometryn                              uuid                                          :func:`regr_sxy`
    :func:`chi_squared_cdf`                   :func:`hmac_sha256`                       :func:`no_values_match`                   st_geometrytype                           value_at_quantile                             :func:`regr_syy`
    :func:`chr`                               :func:`hmac_sha512`                       :func:`none_match`                        st_geomfrombinary                         values_at_quantiles                           :func:`set_agg`
    classify                                  :func:`hour`                              :func:`normal_cdf`                        st_interiorringn                          :func:`week`                                  :func:`set_union`
    :func:`codepoint`                         :func:`infinity`                          normalize                                 st_interiorrings                          :func:`week_of_year`                          :func:`skewness`
    color                                     intersection_cardinality                  now                                       st_intersection                           :func:`weibull_cdf`                           spatial_partitioning
    :func:`combinations`                      :func:`inverse_beta_cdf`                  :func:`parse_datetime`                    st_intersects                             :func:`width_bucket`                          :func:`stddev`
    :func:`concat`                            inverse_binomial_cdf                      parse_duration                            st_isclosed                               :func:`wilson_interval_lower`                 :func:`stddev_pop`
    :func:`contains`                          inverse_cauchy_cdf                        parse_presto_data_size                    st_isempty                                :func:`wilson_interval_upper`                 :func:`stddev_samp`
    :func:`cos`                               inverse_chi_squared_cdf                   :func:`pi`                                st_isring                                 word_stem                                     :func:`sum`
    :func:`cosh`                              inverse_f_cdf                             pinot_binary_decimal_to_double            st_issimple                               :func:`xxhash64`                              tdigest_agg
    :func:`cosine_similarity`                 inverse_gamma_cdf                         :func:`poisson_cdf`                       st_isvalid                                :func:`year`                                  :func:`var_pop`
    :func:`crc32`                             inverse_laplace_cdf                       :func:`pow`                               st_length                                 :func:`year_of_week`                          :func:`var_samp`
    :func:`current_date`                      inverse_normal_cdf                        :func:`power`                             st_linefromtext                           :func:`yow`                                   :func:`variance`
    current_time                              inverse_poisson_cdf                       quantile_at_value                         st_linestring                             :func:`zip`
    current_timestamp                         inverse_weibull_cdf                       :func:`quarter`                           st_multipoint                             :func:`zip_with`
    current_timezone                          ip_prefix                                 :func:`radians`                           st_numgeometries
    :func:`date`                              ip_subnet_max                             :func:`rand`                              st_numinteriorring
    :func:`date_add`                          ip_subnet_min                             :func:`random`                            st_numpoints
    :func:`date_diff`                         ip_subnet_range                           :func:`reduce`                            st_overlaps
    ========================================  ========================================  ========================================  ========================================  ========================================  ==  ========================================  ==  ========================================
