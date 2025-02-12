=================
Function Coverage
=================

Here is a list of all scalar, aggregate, and window functions from Presto, with functions that are available in Velox highlighted.

.. raw:: html

    <style>
    div.body {max-width: 1300px;}
    table.coverage th {background-color: lightblue; text-align: center;}
    table.coverage td:nth-child(6) {background-color: lightblue;}
    table.coverage td:nth-child(8) {background-color: lightblue;}
    table.coverage tr:nth-child(1) td:nth-child(1) {background-color: #6BA81E;}
    table.coverage tr:nth-child(1) td:nth-child(2) {background-color: #6BA81E;}
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
    table.coverage tr:nth-child(5) td:nth-child(3) {background-color: #6BA81E;}
    table.coverage tr:nth-child(5) td:nth-child(4) {background-color: #6BA81E;}
    table.coverage tr:nth-child(5) td:nth-child(7) {background-color: #6BA81E;}
    table.coverage tr:nth-child(5) td:nth-child(9) {background-color: #6BA81E;}
    table.coverage tr:nth-child(6) td:nth-child(1) {background-color: #6BA81E;}
    table.coverage tr:nth-child(6) td:nth-child(2) {background-color: #6BA81E;}
    table.coverage tr:nth-child(6) td:nth-child(4) {background-color: #6BA81E;}
    table.coverage tr:nth-child(6) td:nth-child(7) {background-color: #6BA81E;}
    table.coverage tr:nth-child(6) td:nth-child(9) {background-color: #6BA81E;}
    table.coverage tr:nth-child(7) td:nth-child(1) {background-color: #6BA81E;}
    table.coverage tr:nth-child(7) td:nth-child(2) {background-color: #6BA81E;}
    table.coverage tr:nth-child(7) td:nth-child(4) {background-color: #6BA81E;}
    table.coverage tr:nth-child(7) td:nth-child(7) {background-color: #6BA81E;}
    table.coverage tr:nth-child(7) td:nth-child(9) {background-color: #6BA81E;}
    table.coverage tr:nth-child(8) td:nth-child(1) {background-color: #6BA81E;}
    table.coverage tr:nth-child(8) td:nth-child(2) {background-color: #6BA81E;}
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
    table.coverage tr:nth-child(13) td:nth-child(4) {background-color: #6BA81E;}
    table.coverage tr:nth-child(13) td:nth-child(7) {background-color: #6BA81E;}
    table.coverage tr:nth-child(14) td:nth-child(1) {background-color: #6BA81E;}
    table.coverage tr:nth-child(14) td:nth-child(2) {background-color: #6BA81E;}
    table.coverage tr:nth-child(14) td:nth-child(3) {background-color: #6BA81E;}
    table.coverage tr:nth-child(14) td:nth-child(4) {background-color: #6BA81E;}
    table.coverage tr:nth-child(14) td:nth-child(7) {background-color: #6BA81E;}
    table.coverage tr:nth-child(15) td:nth-child(1) {background-color: #6BA81E;}
    table.coverage tr:nth-child(15) td:nth-child(2) {background-color: #6BA81E;}
    table.coverage tr:nth-child(15) td:nth-child(3) {background-color: #6BA81E;}
    table.coverage tr:nth-child(15) td:nth-child(4) {background-color: #6BA81E;}
    table.coverage tr:nth-child(15) td:nth-child(7) {background-color: #6BA81E;}
    table.coverage tr:nth-child(16) td:nth-child(3) {background-color: #6BA81E;}
    table.coverage tr:nth-child(16) td:nth-child(7) {background-color: #6BA81E;}
    table.coverage tr:nth-child(17) td:nth-child(1) {background-color: #6BA81E;}
    table.coverage tr:nth-child(17) td:nth-child(2) {background-color: #6BA81E;}
    table.coverage tr:nth-child(17) td:nth-child(4) {background-color: #6BA81E;}
    table.coverage tr:nth-child(17) td:nth-child(7) {background-color: #6BA81E;}
    table.coverage tr:nth-child(18) td:nth-child(3) {background-color: #6BA81E;}
    table.coverage tr:nth-child(18) td:nth-child(4) {background-color: #6BA81E;}
    table.coverage tr:nth-child(19) td:nth-child(1) {background-color: #6BA81E;}
    table.coverage tr:nth-child(19) td:nth-child(2) {background-color: #6BA81E;}
    table.coverage tr:nth-child(19) td:nth-child(3) {background-color: #6BA81E;}
    table.coverage tr:nth-child(19) td:nth-child(4) {background-color: #6BA81E;}
    table.coverage tr:nth-child(19) td:nth-child(7) {background-color: #6BA81E;}
    table.coverage tr:nth-child(20) td:nth-child(3) {background-color: #6BA81E;}
    table.coverage tr:nth-child(20) td:nth-child(7) {background-color: #6BA81E;}
    table.coverage tr:nth-child(21) td:nth-child(1) {background-color: #6BA81E;}
    table.coverage tr:nth-child(21) td:nth-child(2) {background-color: #6BA81E;}
    table.coverage tr:nth-child(21) td:nth-child(3) {background-color: #6BA81E;}
    table.coverage tr:nth-child(21) td:nth-child(4) {background-color: #6BA81E;}
    table.coverage tr:nth-child(21) td:nth-child(5) {background-color: #6BA81E;}
    table.coverage tr:nth-child(21) td:nth-child(7) {background-color: #6BA81E;}
    table.coverage tr:nth-child(22) td:nth-child(1) {background-color: #6BA81E;}
    table.coverage tr:nth-child(22) td:nth-child(2) {background-color: #6BA81E;}
    table.coverage tr:nth-child(22) td:nth-child(3) {background-color: #6BA81E;}
    table.coverage tr:nth-child(22) td:nth-child(4) {background-color: #6BA81E;}
    table.coverage tr:nth-child(22) td:nth-child(5) {background-color: #6BA81E;}
    table.coverage tr:nth-child(22) td:nth-child(7) {background-color: #6BA81E;}
    table.coverage tr:nth-child(23) td:nth-child(1) {background-color: #6BA81E;}
    table.coverage tr:nth-child(23) td:nth-child(2) {background-color: #6BA81E;}
    table.coverage tr:nth-child(23) td:nth-child(4) {background-color: #6BA81E;}
    table.coverage tr:nth-child(23) td:nth-child(5) {background-color: #6BA81E;}
    table.coverage tr:nth-child(23) td:nth-child(7) {background-color: #6BA81E;}
    table.coverage tr:nth-child(24) td:nth-child(1) {background-color: #6BA81E;}
    table.coverage tr:nth-child(24) td:nth-child(2) {background-color: #6BA81E;}
    table.coverage tr:nth-child(24) td:nth-child(4) {background-color: #6BA81E;}
    table.coverage tr:nth-child(24) td:nth-child(5) {background-color: #6BA81E;}
    table.coverage tr:nth-child(25) td:nth-child(1) {background-color: #6BA81E;}
    table.coverage tr:nth-child(25) td:nth-child(2) {background-color: #6BA81E;}
    table.coverage tr:nth-child(25) td:nth-child(3) {background-color: #6BA81E;}
    table.coverage tr:nth-child(25) td:nth-child(4) {background-color: #6BA81E;}
    table.coverage tr:nth-child(25) td:nth-child(5) {background-color: #6BA81E;}
    table.coverage tr:nth-child(25) td:nth-child(7) {background-color: #6BA81E;}
    table.coverage tr:nth-child(26) td:nth-child(4) {background-color: #6BA81E;}
    table.coverage tr:nth-child(26) td:nth-child(5) {background-color: #6BA81E;}
    table.coverage tr:nth-child(27) td:nth-child(1) {background-color: #6BA81E;}
    table.coverage tr:nth-child(27) td:nth-child(2) {background-color: #6BA81E;}
    table.coverage tr:nth-child(27) td:nth-child(4) {background-color: #6BA81E;}
    table.coverage tr:nth-child(27) td:nth-child(7) {background-color: #6BA81E;}
    table.coverage tr:nth-child(28) td:nth-child(3) {background-color: #6BA81E;}
    table.coverage tr:nth-child(28) td:nth-child(4) {background-color: #6BA81E;}
    table.coverage tr:nth-child(28) td:nth-child(5) {background-color: #6BA81E;}
    table.coverage tr:nth-child(28) td:nth-child(7) {background-color: #6BA81E;}
    table.coverage tr:nth-child(29) td:nth-child(1) {background-color: #6BA81E;}
    table.coverage tr:nth-child(29) td:nth-child(3) {background-color: #6BA81E;}
    table.coverage tr:nth-child(29) td:nth-child(4) {background-color: #6BA81E;}
    table.coverage tr:nth-child(29) td:nth-child(5) {background-color: #6BA81E;}
    table.coverage tr:nth-child(30) td:nth-child(1) {background-color: #6BA81E;}
    table.coverage tr:nth-child(30) td:nth-child(3) {background-color: #6BA81E;}
    table.coverage tr:nth-child(30) td:nth-child(5) {background-color: #6BA81E;}
    table.coverage tr:nth-child(30) td:nth-child(7) {background-color: #6BA81E;}
    table.coverage tr:nth-child(31) td:nth-child(1) {background-color: #6BA81E;}
    table.coverage tr:nth-child(31) td:nth-child(3) {background-color: #6BA81E;}
    table.coverage tr:nth-child(31) td:nth-child(4) {background-color: #6BA81E;}
    table.coverage tr:nth-child(32) td:nth-child(1) {background-color: #6BA81E;}
    table.coverage tr:nth-child(32) td:nth-child(2) {background-color: #6BA81E;}
    table.coverage tr:nth-child(32) td:nth-child(3) {background-color: #6BA81E;}
    table.coverage tr:nth-child(32) td:nth-child(5) {background-color: #6BA81E;}
    table.coverage tr:nth-child(32) td:nth-child(7) {background-color: #6BA81E;}
    table.coverage tr:nth-child(33) td:nth-child(1) {background-color: #6BA81E;}
    table.coverage tr:nth-child(33) td:nth-child(2) {background-color: #6BA81E;}
    table.coverage tr:nth-child(33) td:nth-child(3) {background-color: #6BA81E;}
    table.coverage tr:nth-child(33) td:nth-child(5) {background-color: #6BA81E;}
    table.coverage tr:nth-child(34) td:nth-child(3) {background-color: #6BA81E;}
    table.coverage tr:nth-child(34) td:nth-child(4) {background-color: #6BA81E;}
    table.coverage tr:nth-child(34) td:nth-child(5) {background-color: #6BA81E;}
    table.coverage tr:nth-child(35) td:nth-child(1) {background-color: #6BA81E;}
    table.coverage tr:nth-child(35) td:nth-child(2) {background-color: #6BA81E;}
    table.coverage tr:nth-child(35) td:nth-child(3) {background-color: #6BA81E;}
    table.coverage tr:nth-child(35) td:nth-child(5) {background-color: #6BA81E;}
    table.coverage tr:nth-child(36) td:nth-child(2) {background-color: #6BA81E;}
    table.coverage tr:nth-child(36) td:nth-child(3) {background-color: #6BA81E;}
    table.coverage tr:nth-child(36) td:nth-child(4) {background-color: #6BA81E;}
    table.coverage tr:nth-child(37) td:nth-child(2) {background-color: #6BA81E;}
    table.coverage tr:nth-child(37) td:nth-child(3) {background-color: #6BA81E;}
    table.coverage tr:nth-child(37) td:nth-child(4) {background-color: #6BA81E;}
    table.coverage tr:nth-child(37) td:nth-child(5) {background-color: #6BA81E;}
    table.coverage tr:nth-child(38) td:nth-child(2) {background-color: #6BA81E;}
    table.coverage tr:nth-child(38) td:nth-child(3) {background-color: #6BA81E;}
    table.coverage tr:nth-child(38) td:nth-child(4) {background-color: #6BA81E;}
    table.coverage tr:nth-child(38) td:nth-child(5) {background-color: #6BA81E;}
    table.coverage tr:nth-child(38) td:nth-child(7) {background-color: #6BA81E;}
    table.coverage tr:nth-child(39) td:nth-child(2) {background-color: #6BA81E;}
    table.coverage tr:nth-child(39) td:nth-child(5) {background-color: #6BA81E;}
    table.coverage tr:nth-child(39) td:nth-child(7) {background-color: #6BA81E;}
    table.coverage tr:nth-child(40) td:nth-child(2) {background-color: #6BA81E;}
    table.coverage tr:nth-child(40) td:nth-child(3) {background-color: #6BA81E;}
    table.coverage tr:nth-child(40) td:nth-child(4) {background-color: #6BA81E;}
    table.coverage tr:nth-child(40) td:nth-child(5) {background-color: #6BA81E;}
    table.coverage tr:nth-child(40) td:nth-child(7) {background-color: #6BA81E;}
    table.coverage tr:nth-child(41) td:nth-child(2) {background-color: #6BA81E;}
    table.coverage tr:nth-child(41) td:nth-child(3) {background-color: #6BA81E;}
    table.coverage tr:nth-child(41) td:nth-child(4) {background-color: #6BA81E;}
    table.coverage tr:nth-child(41) td:nth-child(5) {background-color: #6BA81E;}
    table.coverage tr:nth-child(41) td:nth-child(7) {background-color: #6BA81E;}
    table.coverage tr:nth-child(42) td:nth-child(2) {background-color: #6BA81E;}
    table.coverage tr:nth-child(42) td:nth-child(3) {background-color: #6BA81E;}
    table.coverage tr:nth-child(42) td:nth-child(4) {background-color: #6BA81E;}
    table.coverage tr:nth-child(42) td:nth-child(7) {background-color: #6BA81E;}
    table.coverage tr:nth-child(43) td:nth-child(2) {background-color: #6BA81E;}
    table.coverage tr:nth-child(43) td:nth-child(3) {background-color: #6BA81E;}
    table.coverage tr:nth-child(43) td:nth-child(5) {background-color: #6BA81E;}
    table.coverage tr:nth-child(43) td:nth-child(7) {background-color: #6BA81E;}
    table.coverage tr:nth-child(44) td:nth-child(2) {background-color: #6BA81E;}
    table.coverage tr:nth-child(44) td:nth-child(3) {background-color: #6BA81E;}
    table.coverage tr:nth-child(44) td:nth-child(5) {background-color: #6BA81E;}
    table.coverage tr:nth-child(45) td:nth-child(1) {background-color: #6BA81E;}
    table.coverage tr:nth-child(45) td:nth-child(2) {background-color: #6BA81E;}
    table.coverage tr:nth-child(45) td:nth-child(5) {background-color: #6BA81E;}
    table.coverage tr:nth-child(45) td:nth-child(7) {background-color: #6BA81E;}
    table.coverage tr:nth-child(46) td:nth-child(1) {background-color: #6BA81E;}
    table.coverage tr:nth-child(46) td:nth-child(2) {background-color: #6BA81E;}
    table.coverage tr:nth-child(46) td:nth-child(5) {background-color: #6BA81E;}
    table.coverage tr:nth-child(46) td:nth-child(7) {background-color: #6BA81E;}
    table.coverage tr:nth-child(47) td:nth-child(1) {background-color: #6BA81E;}
    table.coverage tr:nth-child(47) td:nth-child(3) {background-color: #6BA81E;}
    table.coverage tr:nth-child(47) td:nth-child(5) {background-color: #6BA81E;}
    table.coverage tr:nth-child(47) td:nth-child(7) {background-color: #6BA81E;}
    table.coverage tr:nth-child(48) td:nth-child(1) {background-color: #6BA81E;}
    table.coverage tr:nth-child(48) td:nth-child(3) {background-color: #6BA81E;}
    table.coverage tr:nth-child(48) td:nth-child(5) {background-color: #6BA81E;}
    table.coverage tr:nth-child(49) td:nth-child(1) {background-color: #6BA81E;}
    table.coverage tr:nth-child(49) td:nth-child(3) {background-color: #6BA81E;}
    table.coverage tr:nth-child(49) td:nth-child(5) {background-color: #6BA81E;}
    table.coverage tr:nth-child(50) td:nth-child(1) {background-color: #6BA81E;}
    table.coverage tr:nth-child(50) td:nth-child(5) {background-color: #6BA81E;}
    table.coverage tr:nth-child(51) td:nth-child(1) {background-color: #6BA81E;}
    table.coverage tr:nth-child(51) td:nth-child(5) {background-color: #6BA81E;}
    table.coverage tr:nth-child(52) td:nth-child(1) {background-color: #6BA81E;}
    table.coverage tr:nth-child(52) td:nth-child(3) {background-color: #6BA81E;}
    table.coverage tr:nth-child(52) td:nth-child(5) {background-color: #6BA81E;}
    table.coverage tr:nth-child(53) td:nth-child(1) {background-color: #6BA81E;}
    table.coverage tr:nth-child(53) td:nth-child(3) {background-color: #6BA81E;}
    table.coverage tr:nth-child(54) td:nth-child(1) {background-color: #6BA81E;}
    table.coverage tr:nth-child(54) td:nth-child(3) {background-color: #6BA81E;}
    table.coverage tr:nth-child(54) td:nth-child(5) {background-color: #6BA81E;}
    table.coverage tr:nth-child(54) td:nth-child(7) {background-color: #6BA81E;}
    table.coverage tr:nth-child(55) td:nth-child(1) {background-color: #6BA81E;}
    table.coverage tr:nth-child(55) td:nth-child(2) {background-color: #6BA81E;}
    table.coverage tr:nth-child(55) td:nth-child(3) {background-color: #6BA81E;}
    table.coverage tr:nth-child(55) td:nth-child(5) {background-color: #6BA81E;}
    table.coverage tr:nth-child(55) td:nth-child(7) {background-color: #6BA81E;}
    table.coverage tr:nth-child(56) td:nth-child(1) {background-color: #6BA81E;}
    table.coverage tr:nth-child(56) td:nth-child(2) {background-color: #6BA81E;}
    table.coverage tr:nth-child(56) td:nth-child(3) {background-color: #6BA81E;}
    table.coverage tr:nth-child(56) td:nth-child(5) {background-color: #6BA81E;}
    table.coverage tr:nth-child(56) td:nth-child(7) {background-color: #6BA81E;}
    table.coverage tr:nth-child(57) td:nth-child(1) {background-color: #6BA81E;}
    table.coverage tr:nth-child(57) td:nth-child(5) {background-color: #6BA81E;}
    table.coverage tr:nth-child(57) td:nth-child(7) {background-color: #6BA81E;}
    table.coverage tr:nth-child(58) td:nth-child(1) {background-color: #6BA81E;}
    table.coverage tr:nth-child(58) td:nth-child(2) {background-color: #6BA81E;}
    table.coverage tr:nth-child(58) td:nth-child(5) {background-color: #6BA81E;}
    table.coverage tr:nth-child(58) td:nth-child(7) {background-color: #6BA81E;}
    table.coverage tr:nth-child(59) td:nth-child(1) {background-color: #6BA81E;}
    table.coverage tr:nth-child(59) td:nth-child(2) {background-color: #6BA81E;}
    table.coverage tr:nth-child(59) td:nth-child(5) {background-color: #6BA81E;}
    table.coverage tr:nth-child(59) td:nth-child(7) {background-color: #6BA81E;}
    table.coverage tr:nth-child(60) td:nth-child(1) {background-color: #6BA81E;}
    table.coverage tr:nth-child(60) td:nth-child(2) {background-color: #6BA81E;}
    table.coverage tr:nth-child(60) td:nth-child(3) {background-color: #6BA81E;}
    table.coverage tr:nth-child(60) td:nth-child(5) {background-color: #6BA81E;}
    table.coverage tr:nth-child(60) td:nth-child(7) {background-color: #6BA81E;}
    table.coverage tr:nth-child(61) td:nth-child(1) {background-color: #6BA81E;}
    table.coverage tr:nth-child(61) td:nth-child(2) {background-color: #6BA81E;}
    table.coverage tr:nth-child(61) td:nth-child(3) {background-color: #6BA81E;}
    table.coverage tr:nth-child(61) td:nth-child(5) {background-color: #6BA81E;}
    table.coverage tr:nth-child(61) td:nth-child(7) {background-color: #6BA81E;}
    table.coverage tr:nth-child(62) td:nth-child(1) {background-color: #6BA81E;}
    table.coverage tr:nth-child(62) td:nth-child(2) {background-color: #6BA81E;}
    table.coverage tr:nth-child(62) td:nth-child(3) {background-color: #6BA81E;}
    table.coverage tr:nth-child(62) td:nth-child(5) {background-color: #6BA81E;}
    table.coverage tr:nth-child(62) td:nth-child(7) {background-color: #6BA81E;}
    table.coverage tr:nth-child(63) td:nth-child(1) {background-color: #6BA81E;}
    table.coverage tr:nth-child(63) td:nth-child(2) {background-color: #6BA81E;}
    table.coverage tr:nth-child(63) td:nth-child(3) {background-color: #6BA81E;}
    table.coverage tr:nth-child(63) td:nth-child(5) {background-color: #6BA81E;}
    table.coverage tr:nth-child(63) td:nth-child(7) {background-color: #6BA81E;}
    table.coverage tr:nth-child(64) td:nth-child(3) {background-color: #6BA81E;}
    table.coverage tr:nth-child(64) td:nth-child(5) {background-color: #6BA81E;}
    table.coverage tr:nth-child(65) td:nth-child(1) {background-color: #6BA81E;}
    table.coverage tr:nth-child(65) td:nth-child(2) {background-color: #6BA81E;}
    table.coverage tr:nth-child(65) td:nth-child(3) {background-color: #6BA81E;}
    table.coverage tr:nth-child(65) td:nth-child(7) {background-color: #6BA81E;}
    table.coverage tr:nth-child(66) td:nth-child(3) {background-color: #6BA81E;}
    table.coverage tr:nth-child(66) td:nth-child(7) {background-color: #6BA81E;}
    table.coverage tr:nth-child(67) td:nth-child(1) {background-color: #6BA81E;}
    table.coverage tr:nth-child(67) td:nth-child(2) {background-color: #6BA81E;}
    table.coverage tr:nth-child(67) td:nth-child(5) {background-color: #6BA81E;}
    table.coverage tr:nth-child(68) td:nth-child(1) {background-color: #6BA81E;}
    table.coverage tr:nth-child(68) td:nth-child(3) {background-color: #6BA81E;}
    table.coverage tr:nth-child(68) td:nth-child(5) {background-color: #6BA81E;}
    table.coverage tr:nth-child(69) td:nth-child(1) {background-color: #6BA81E;}
    table.coverage tr:nth-child(69) td:nth-child(5) {background-color: #6BA81E;}
    table.coverage tr:nth-child(69) td:nth-child(7) {background-color: #6BA81E;}
    table.coverage tr:nth-child(70) td:nth-child(1) {background-color: #6BA81E;}
    table.coverage tr:nth-child(70) td:nth-child(5) {background-color: #6BA81E;}
    table.coverage tr:nth-child(71) td:nth-child(1) {background-color: #6BA81E;}
    table.coverage tr:nth-child(71) td:nth-child(2) {background-color: #6BA81E;}
    table.coverage tr:nth-child(71) td:nth-child(3) {background-color: #6BA81E;}
    table.coverage tr:nth-child(71) td:nth-child(5) {background-color: #6BA81E;}
    table.coverage tr:nth-child(71) td:nth-child(7) {background-color: #6BA81E;}
    table.coverage tr:nth-child(72) td:nth-child(1) {background-color: #6BA81E;}
    table.coverage tr:nth-child(72) td:nth-child(2) {background-color: #6BA81E;}
    table.coverage tr:nth-child(72) td:nth-child(5) {background-color: #6BA81E;}
    table.coverage tr:nth-child(72) td:nth-child(7) {background-color: #6BA81E;}
    table.coverage tr:nth-child(73) td:nth-child(1) {background-color: #6BA81E;}
    table.coverage tr:nth-child(73) td:nth-child(3) {background-color: #6BA81E;}
    table.coverage tr:nth-child(73) td:nth-child(5) {background-color: #6BA81E;}
    table.coverage tr:nth-child(73) td:nth-child(7) {background-color: #6BA81E;}
    table.coverage tr:nth-child(74) td:nth-child(1) {background-color: #6BA81E;}
    table.coverage tr:nth-child(74) td:nth-child(2) {background-color: #6BA81E;}
    table.coverage tr:nth-child(74) td:nth-child(3) {background-color: #6BA81E;}
    table.coverage tr:nth-child(74) td:nth-child(5) {background-color: #6BA81E;}
    table.coverage tr:nth-child(74) td:nth-child(7) {background-color: #6BA81E;}
    table.coverage tr:nth-child(75) td:nth-child(2) {background-color: #6BA81E;}
    table.coverage tr:nth-child(75) td:nth-child(3) {background-color: #6BA81E;}
    table.coverage tr:nth-child(75) td:nth-child(5) {background-color: #6BA81E;}
    table.coverage tr:nth-child(76) td:nth-child(5) {background-color: #6BA81E;}
    table.coverage tr:nth-child(76) td:nth-child(7) {background-color: #6BA81E;}
    table.coverage tr:nth-child(77) td:nth-child(3) {background-color: #6BA81E;}
    table.coverage tr:nth-child(77) td:nth-child(5) {background-color: #6BA81E;}
    table.coverage tr:nth-child(77) td:nth-child(7) {background-color: #6BA81E;}
    table.coverage tr:nth-child(78) td:nth-child(1) {background-color: #6BA81E;}
    table.coverage tr:nth-child(78) td:nth-child(3) {background-color: #6BA81E;}
    table.coverage tr:nth-child(78) td:nth-child(5) {background-color: #6BA81E;}
    table.coverage tr:nth-child(78) td:nth-child(7) {background-color: #6BA81E;}
    table.coverage tr:nth-child(79) td:nth-child(1) {background-color: #6BA81E;}
    table.coverage tr:nth-child(79) td:nth-child(3) {background-color: #6BA81E;}
    table.coverage tr:nth-child(79) td:nth-child(5) {background-color: #6BA81E;}
    </style>

.. table::
    :widths: auto
    :class: coverage

    ========================================  ========================================  ========================================  ========================================  ========================================  ==  ========================================  ==  ========================================
    Scalar Functions                                                                                                                                                                                                      Aggregate Functions                           Window Functions
    ================================================================================================================================================================================================================  ==  ========================================  ==  ========================================
    :func:`abs`                               :func:`date_diff`                         ip_subnet_range                           :func:`random`                            st_numgeometries                              :func:`approx_distinct`                       :func:`cume_dist`
    :func:`acos`                              :func:`date_format`                       :func:`is_finite`                         :func:`reduce`                            st_numinteriorring                            :func:`approx_most_frequent`                  :func:`dense_rank`
    :func:`all_match`                         :func:`date_parse`                        :func:`is_infinite`                       :func:`regexp_extract`                    st_numpoints                                  :func:`approx_percentile`                     :func:`first_value`
    :func:`any_keys_match`                    :func:`date_trunc`                        :func:`is_json_scalar`                    :func:`regexp_extract_all`                st_overlaps                                   :func:`approx_set`                            :func:`lag`
    :func:`any_match`                         :func:`day`                               :func:`is_nan`                            :func:`regexp_like`                       st_point                                      :func:`arbitrary`                             :func:`last_value`
    :func:`any_values_match`                  :func:`day_of_month`                      is_private_ip                             :func:`regexp_replace`                    st_pointn                                     :func:`array_agg`                             :func:`lead`
    :func:`array_average`                     :func:`day_of_week`                       is_subnet_of                              :func:`regexp_split`                      st_points                                     :func:`avg`                                   :func:`nth_value`
    :func:`array_cum_sum`                     :func:`day_of_year`                       jaccard_index                             regress                                   st_polygon                                    :func:`bitwise_and_agg`                       :func:`ntile`
    :func:`array_distinct`                    :func:`degrees`                           :func:`json_array_contains`               reidentification_potential                st_relate                                     :func:`bitwise_or_agg`                        :func:`percent_rank`
    :func:`array_duplicates`                  :func:`dow`                               :func:`json_array_get`                    :func:`remove_nulls`                      st_startpoint                                 :func:`bool_and`                              :func:`rank`
    :func:`array_except`                      :func:`doy`                               :func:`json_array_length`                 render                                    st_symdifference                              :func:`bool_or`                               :func:`row_number`
    :func:`array_frequency`                   :func:`e`                                 :func:`json_extract`                      :func:`repeat`                            st_touches                                    :func:`checksum`
    :func:`array_has_duplicates`              :func:`element_at`                        :func:`json_extract_scalar`               :func:`replace`                           st_union                                      :func:`classification_fall_out`
    :func:`array_intersect`                   :func:`empty_approx_set`                  :func:`json_format`                       :func:`replace_first`                     st_within                                     :func:`classification_miss_rate`
    :func:`array_join`                        :func:`ends_with`                         :func:`json_parse`                        :func:`reverse`                           st_x                                          :func:`classification_precision`
    array_least_frequent                      enum_key                                  :func:`json_size`                         rgb                                       st_xmax                                       :func:`classification_recall`
    :func:`array_max`                         :func:`exp`                               key_sampling_percent                      :func:`round`                             st_xmin                                       :func:`classification_thresholds`
    array_max_by                              expand_envelope                           :func:`laplace_cdf`                       :func:`rpad`                              st_y                                          convex_hull_agg
    :func:`array_min`                         :func:`f_cdf`                             :func:`last_day_of_month`                 :func:`rtrim`                             st_ymax                                       :func:`corr`
    array_min_by                              features                                  :func:`least`                             scale_qdigest                             st_ymin                                       :func:`count`
    :func:`array_normalize`                   :func:`filter`                            :func:`length`                            :func:`second`                            :func:`starts_with`                           :func:`count_if`
    :func:`array_position`                    :func:`filter`                            :func:`levenshtein_distance`              :func:`secure_rand`                       :func:`strpos`                                :func:`covar_pop`
    :func:`array_remove`                      :func:`find_first`                        line_interpolate_point                    :func:`secure_random`                     :func:`strrpos`                               :func:`covar_samp`
    :func:`array_sort`                        :func:`find_first_index`                  line_locate_point                         :func:`sequence`                          :func:`substr`                                differential_entropy
    :func:`array_sort_desc`                   :func:`flatten`                           :func:`ln`                                :func:`sha1`                              :func:`tan`                                   :func:`entropy`
    array_split_into_chunks                   flatten_geometry_collections              localtime                                 :func:`sha256`                            :func:`tanh`                                  evaluate_classifier_predictions
    :func:`array_sum`                         :func:`floor`                             localtimestamp                            :func:`sha512`                            tdigest_agg                                   :func:`every`
    array_top_n                               fnv1_32                                   :func:`log10`                             :func:`shuffle`                           :func:`timezone_hour`                         :func:`geometric_mean`
    :func:`array_union`                       fnv1_64                                   :func:`log2`                              :func:`sign`                              :func:`timezone_minute`                       geometry_union_agg
    :func:`arrays_overlap`                    fnv1a_32                                  :func:`lower`                             simplify_geometry                         :func:`to_base`                               :func:`histogram`
    :func:`asin`                              fnv1a_64                                  :func:`lpad`                              :func:`sin`                               to_base32                                     khyperloglog_agg
    :func:`atan`                              :func:`format_datetime`                   :func:`ltrim`                             sketch_kll_quantile                       :func:`to_base64`                             :func:`kurtosis`
    :func:`atan2`                             :func:`from_base`                         :func:`map`                               sketch_kll_rank                           :func:`to_base64url`                          learn_classifier
    bar                                       from_base32                               :func:`map_concat`                        :func:`slice`                             :func:`to_big_endian_32`                      learn_libsvm_classifier
    :func:`beta_cdf`                          :func:`from_base64`                       :func:`map_entries`                       spatial_partitions                        :func:`to_big_endian_64`                      learn_libsvm_regressor
    bing_tile                                 :func:`from_base64url`                    :func:`map_filter`                        :func:`split`                             to_geometry                                   learn_regressor
    bing_tile_at                              :func:`from_big_endian_32`                :func:`map_from_entries`                  :func:`split_part`                        :func:`to_hex`                                make_set_digest
    bing_tile_children                        :func:`from_big_endian_64`                :func:`map_keys`                          :func:`split_to_map`                      :func:`to_ieee754_32`                         :func:`map_agg`
    bing_tile_coordinates                     :func:`from_hex`                          map_keys_by_top_n_values                  split_to_multimap                         :func:`to_ieee754_64`                         :func:`map_union`
    bing_tile_parent                          :func:`from_ieee754_32`                   :func:`map_normalize`                     :func:`spooky_hash_v2_32`                 :func:`to_iso8601`                            :func:`map_union_sum`
    bing_tile_polygon                         :func:`from_ieee754_64`                   :func:`map_remove_null_values`            :func:`spooky_hash_v2_64`                 :func:`to_milliseconds`                       :func:`max`
    bing_tile_quadkey                         :func:`from_iso8601_date`                 :func:`map_subset`                        :func:`sqrt`                              to_spherical_geography                        :func:`max_by`
    bing_tile_zoom_level                      :func:`from_iso8601_timestamp`            :func:`map_top_n`                         st_area                                   :func:`to_unixtime`                           :func:`merge`
    bing_tiles_around                         :func:`from_unixtime`                     :func:`map_top_n_keys`                    st_asbinary                               :func:`to_utf8`                               merge_set_digest
    :func:`binomial_cdf`                      :func:`from_utf8`                         map_top_n_keys_by_value                   st_astext                                 :func:`trail`                                 :func:`min`
    :func:`bit_count`                         :func:`gamma_cdf`                         map_top_n_values                          st_boundary                               :func:`transform`                             :func:`min_by`
    :func:`bitwise_and`                       geometry_as_geojson                       :func:`map_values`                        st_buffer                                 :func:`transform_keys`                        :func:`multimap_agg`
    :func:`bitwise_arithmetic_shift_right`    geometry_from_geojson                     :func:`map_zip_with`                      st_centroid                               :func:`transform_values`                      noisy_avg_gaussian
    :func:`bitwise_left_shift`                geometry_invalid_reason                   :func:`md5`                               st_contains                               :func:`trim`                                  noisy_count_gaussian
    :func:`bitwise_logical_shift_right`       geometry_nearest_points                   merge_hll                                 st_convexhull                             :func:`trim_array`                            noisy_count_if_gaussian
    :func:`bitwise_not`                       geometry_to_bing_tiles                    merge_khll                                st_coorddim                               :func:`truncate`                              noisy_sum_gaussian
    :func:`bitwise_or`                        geometry_to_dissolved_bing_tiles          :func:`millisecond`                       st_crosses                                :func:`typeof`                                numeric_histogram
    :func:`bitwise_right_shift`               geometry_union                            :func:`minute`                            st_difference                             uniqueness_distribution                       qdigest_agg
    :func:`bitwise_right_shift_arithmetic`    great_circle_distance                     :func:`mod`                               st_dimension                              :func:`upper`                                 :func:`reduce_agg`
    :func:`bitwise_shift_left`                :func:`greatest`                          :func:`month`                             st_disjoint                               :func:`url_decode`                            :func:`regr_avgx`
    :func:`bitwise_xor`                       :func:`hamming_distance`                  :func:`multimap_from_entries`             st_distance                               :func:`url_encode`                            :func:`regr_avgy`
    :func:`cardinality`                       hash_counts                               murmur3_x64_128                           st_endpoint                               :func:`url_extract_fragment`                  :func:`regr_count`
    :func:`cauchy_cdf`                        :func:`hmac_md5`                          myanmar_font_encoding                     st_envelope                               :func:`url_extract_host`                      :func:`regr_intercept`
    :func:`cbrt`                              :func:`hmac_sha1`                         myanmar_normalize_unicode                 st_envelopeaspts                          :func:`url_extract_parameter`                 :func:`regr_r2`
    :func:`ceil`                              :func:`hmac_sha256`                       :func:`nan`                               st_equals                                 :func:`url_extract_path`                      :func:`regr_slope`
    :func:`ceiling`                           :func:`hmac_sha512`                       :func:`ngrams`                            st_exteriorring                           :func:`url_extract_port`                      :func:`regr_sxx`
    :func:`chi_squared_cdf`                   :func:`hour`                              :func:`no_keys_match`                     st_geometries                             :func:`url_extract_protocol`                  :func:`regr_sxy`
    :func:`chr`                               :func:`infinity`                          :func:`no_values_match`                   st_geometryfromtext                       :func:`url_extract_query`                     :func:`regr_syy`
    classify                                  intersection_cardinality                  :func:`none_match`                        st_geometryn                              :func:`uuid`                                  reservoir_sample
    :func:`codepoint`                         :func:`inverse_beta_cdf`                  :func:`normal_cdf`                        st_geometrytype                           value_at_quantile                             :func:`set_agg`
    color                                     inverse_binomial_cdf                      :func:`normalize`                         st_geomfrombinary                         values_at_quantiles                           :func:`set_union`
    :func:`combinations`                      :func:`inverse_cauchy_cdf`                now                                       st_interiorringn                          :func:`week`                                  sketch_kll
    :func:`concat`                            inverse_chi_squared_cdf                   :func:`parse_datetime`                    st_interiorrings                          :func:`week_of_year`                          sketch_kll_with_k
    :func:`contains`                          inverse_f_cdf                             parse_duration                            st_intersection                           :func:`weibull_cdf`                           :func:`skewness`
    :func:`cos`                               inverse_gamma_cdf                         parse_presto_data_size                    st_intersects                             :func:`width_bucket`                          spatial_partitioning
    :func:`cosh`                              :func:`inverse_laplace_cdf`               :func:`pi`                                st_isclosed                               :func:`wilson_interval_lower`                 :func:`stddev`
    :func:`cosine_similarity`                 :func:`inverse_normal_cdf`                pinot_binary_decimal_to_double            st_isempty                                :func:`wilson_interval_upper`                 :func:`stddev_pop`
    :func:`crc32`                             inverse_poisson_cdf                       :func:`poisson_cdf`                       st_isring                                 :func:`word_stem`                             :func:`stddev_samp`
    :func:`current_date`                      :func:`inverse_weibull_cdf`               :func:`pow`                               st_issimple                               :func:`xxhash64`                              :func:`sum`
    current_time                              :func:`ip_prefix`                         :func:`power`                             st_isvalid                                :func:`year`                                  tdigest_agg
    current_timestamp                         ip_prefix_collapse                        quantile_at_value                         st_length                                 :func:`year_of_week`                          :func:`var_pop`
    current_timezone                          ip_prefix_subnets                         :func:`quarter`                           st_linefromtext                           :func:`yow`                                   :func:`var_samp`
    :func:`date`                              ip_subnet_max                             :func:`radians`                           st_linestring                             :func:`zip`                                   :func:`variance`
    :func:`date_add`                          ip_subnet_min                             :func:`rand`                              st_multipoint                             :func:`zip_with`
    ========================================  ========================================  ========================================  ========================================  ========================================  ==  ========================================  ==  ========================================
