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
    table.coverage tr:nth-child(12) td:nth-child(7) {background-color: #6BA81E;}
    table.coverage tr:nth-child(13) td:nth-child(1) {background-color: #6BA81E;}
    table.coverage tr:nth-child(13) td:nth-child(2) {background-color: #6BA81E;}
    table.coverage tr:nth-child(13) td:nth-child(3) {background-color: #6BA81E;}
    table.coverage tr:nth-child(13) td:nth-child(4) {background-color: #6BA81E;}
    table.coverage tr:nth-child(14) td:nth-child(1) {background-color: #6BA81E;}
    table.coverage tr:nth-child(14) td:nth-child(2) {background-color: #6BA81E;}
    table.coverage tr:nth-child(14) td:nth-child(3) {background-color: #6BA81E;}
    table.coverage tr:nth-child(15) td:nth-child(1) {background-color: #6BA81E;}
    table.coverage tr:nth-child(15) td:nth-child(2) {background-color: #6BA81E;}
    table.coverage tr:nth-child(15) td:nth-child(4) {background-color: #6BA81E;}
    table.coverage tr:nth-child(16) td:nth-child(3) {background-color: #6BA81E;}
    table.coverage tr:nth-child(16) td:nth-child(4) {background-color: #6BA81E;}
    table.coverage tr:nth-child(17) td:nth-child(1) {background-color: #6BA81E;}
    table.coverage tr:nth-child(17) td:nth-child(2) {background-color: #6BA81E;}
    table.coverage tr:nth-child(17) td:nth-child(3) {background-color: #6BA81E;}
    table.coverage tr:nth-child(17) td:nth-child(4) {background-color: #6BA81E;}
    table.coverage tr:nth-child(18) td:nth-child(3) {background-color: #6BA81E;}
    table.coverage tr:nth-child(18) td:nth-child(5) {background-color: #6BA81E;}
    table.coverage tr:nth-child(19) td:nth-child(1) {background-color: #6BA81E;}
    table.coverage tr:nth-child(19) td:nth-child(2) {background-color: #6BA81E;}
    table.coverage tr:nth-child(19) td:nth-child(3) {background-color: #6BA81E;}
    table.coverage tr:nth-child(19) td:nth-child(4) {background-color: #6BA81E;}
    table.coverage tr:nth-child(19) td:nth-child(5) {background-color: #6BA81E;}
    table.coverage tr:nth-child(19) td:nth-child(7) {background-color: #6BA81E;}
    table.coverage tr:nth-child(20) td:nth-child(3) {background-color: #6BA81E;}
    table.coverage tr:nth-child(20) td:nth-child(5) {background-color: #6BA81E;}
    table.coverage tr:nth-child(20) td:nth-child(7) {background-color: #6BA81E;}
    table.coverage tr:nth-child(21) td:nth-child(1) {background-color: #6BA81E;}
    table.coverage tr:nth-child(21) td:nth-child(2) {background-color: #6BA81E;}
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
    table.coverage tr:nth-child(23) td:nth-child(5) {background-color: #6BA81E;}
    table.coverage tr:nth-child(23) td:nth-child(7) {background-color: #6BA81E;}
    table.coverage tr:nth-child(24) td:nth-child(1) {background-color: #6BA81E;}
    table.coverage tr:nth-child(24) td:nth-child(2) {background-color: #6BA81E;}
    table.coverage tr:nth-child(24) td:nth-child(4) {background-color: #6BA81E;}
    table.coverage tr:nth-child(25) td:nth-child(1) {background-color: #6BA81E;}
    table.coverage tr:nth-child(25) td:nth-child(2) {background-color: #6BA81E;}
    table.coverage tr:nth-child(25) td:nth-child(4) {background-color: #6BA81E;}
    table.coverage tr:nth-child(25) td:nth-child(5) {background-color: #6BA81E;}
    table.coverage tr:nth-child(25) td:nth-child(7) {background-color: #6BA81E;}
    table.coverage tr:nth-child(26) td:nth-child(1) {background-color: #6BA81E;}
    table.coverage tr:nth-child(26) td:nth-child(3) {background-color: #6BA81E;}
    table.coverage tr:nth-child(26) td:nth-child(4) {background-color: #6BA81E;}
    table.coverage tr:nth-child(26) td:nth-child(5) {background-color: #6BA81E;}
    table.coverage tr:nth-child(27) td:nth-child(2) {background-color: #6BA81E;}
    table.coverage tr:nth-child(27) td:nth-child(3) {background-color: #6BA81E;}
    table.coverage tr:nth-child(27) td:nth-child(4) {background-color: #6BA81E;}
    table.coverage tr:nth-child(27) td:nth-child(5) {background-color: #6BA81E;}
    table.coverage tr:nth-child(27) td:nth-child(7) {background-color: #6BA81E;}
    table.coverage tr:nth-child(28) td:nth-child(1) {background-color: #6BA81E;}
    table.coverage tr:nth-child(28) td:nth-child(3) {background-color: #6BA81E;}
    table.coverage tr:nth-child(28) td:nth-child(7) {background-color: #6BA81E;}
    table.coverage tr:nth-child(29) td:nth-child(1) {background-color: #6BA81E;}
    table.coverage tr:nth-child(29) td:nth-child(3) {background-color: #6BA81E;}
    table.coverage tr:nth-child(29) td:nth-child(4) {background-color: #6BA81E;}
    table.coverage tr:nth-child(29) td:nth-child(5) {background-color: #6BA81E;}
    table.coverage tr:nth-child(30) td:nth-child(1) {background-color: #6BA81E;}
    table.coverage tr:nth-child(30) td:nth-child(3) {background-color: #6BA81E;}
    table.coverage tr:nth-child(30) td:nth-child(4) {background-color: #6BA81E;}
    table.coverage tr:nth-child(30) td:nth-child(5) {background-color: #6BA81E;}
    table.coverage tr:nth-child(30) td:nth-child(7) {background-color: #6BA81E;}
    table.coverage tr:nth-child(31) td:nth-child(1) {background-color: #6BA81E;}
    table.coverage tr:nth-child(31) td:nth-child(3) {background-color: #6BA81E;}
    table.coverage tr:nth-child(31) td:nth-child(5) {background-color: #6BA81E;}
    table.coverage tr:nth-child(32) td:nth-child(1) {background-color: #6BA81E;}
    table.coverage tr:nth-child(32) td:nth-child(2) {background-color: #6BA81E;}
    table.coverage tr:nth-child(32) td:nth-child(3) {background-color: #6BA81E;}
    table.coverage tr:nth-child(32) td:nth-child(4) {background-color: #6BA81E;}
    table.coverage tr:nth-child(32) td:nth-child(5) {background-color: #6BA81E;}
    table.coverage tr:nth-child(32) td:nth-child(7) {background-color: #6BA81E;}
    table.coverage tr:nth-child(33) td:nth-child(2) {background-color: #6BA81E;}
    table.coverage tr:nth-child(33) td:nth-child(3) {background-color: #6BA81E;}
    table.coverage tr:nth-child(33) td:nth-child(4) {background-color: #6BA81E;}
    table.coverage tr:nth-child(34) td:nth-child(1) {background-color: #6BA81E;}
    table.coverage tr:nth-child(34) td:nth-child(3) {background-color: #6BA81E;}
    table.coverage tr:nth-child(34) td:nth-child(4) {background-color: #6BA81E;}
    table.coverage tr:nth-child(34) td:nth-child(5) {background-color: #6BA81E;}
    table.coverage tr:nth-child(35) td:nth-child(2) {background-color: #6BA81E;}
    table.coverage tr:nth-child(35) td:nth-child(3) {background-color: #6BA81E;}
    table.coverage tr:nth-child(35) td:nth-child(5) {background-color: #6BA81E;}
    table.coverage tr:nth-child(36) td:nth-child(2) {background-color: #6BA81E;}
    table.coverage tr:nth-child(36) td:nth-child(3) {background-color: #6BA81E;}
    table.coverage tr:nth-child(36) td:nth-child(4) {background-color: #6BA81E;}
    table.coverage tr:nth-child(36) td:nth-child(5) {background-color: #6BA81E;}
    table.coverage tr:nth-child(37) td:nth-child(2) {background-color: #6BA81E;}
    table.coverage tr:nth-child(37) td:nth-child(4) {background-color: #6BA81E;}
    table.coverage tr:nth-child(38) td:nth-child(2) {background-color: #6BA81E;}
    table.coverage tr:nth-child(38) td:nth-child(3) {background-color: #6BA81E;}
    table.coverage tr:nth-child(38) td:nth-child(4) {background-color: #6BA81E;}
    table.coverage tr:nth-child(38) td:nth-child(7) {background-color: #6BA81E;}
    table.coverage tr:nth-child(39) td:nth-child(2) {background-color: #6BA81E;}
    table.coverage tr:nth-child(39) td:nth-child(7) {background-color: #6BA81E;}
    table.coverage tr:nth-child(40) td:nth-child(2) {background-color: #6BA81E;}
    table.coverage tr:nth-child(40) td:nth-child(3) {background-color: #6BA81E;}
    table.coverage tr:nth-child(40) td:nth-child(5) {background-color: #6BA81E;}
    table.coverage tr:nth-child(40) td:nth-child(7) {background-color: #6BA81E;}
    table.coverage tr:nth-child(41) td:nth-child(2) {background-color: #6BA81E;}
    table.coverage tr:nth-child(41) td:nth-child(3) {background-color: #6BA81E;}
    table.coverage tr:nth-child(41) td:nth-child(5) {background-color: #6BA81E;}
    table.coverage tr:nth-child(41) td:nth-child(7) {background-color: #6BA81E;}
    table.coverage tr:nth-child(42) td:nth-child(2) {background-color: #6BA81E;}
    table.coverage tr:nth-child(42) td:nth-child(7) {background-color: #6BA81E;}
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
    table.coverage tr:nth-child(46) td:nth-child(2) {background-color: #6BA81E;}
    table.coverage tr:nth-child(46) td:nth-child(3) {background-color: #6BA81E;}
    table.coverage tr:nth-child(46) td:nth-child(5) {background-color: #6BA81E;}
    table.coverage tr:nth-child(46) td:nth-child(7) {background-color: #6BA81E;}
    table.coverage tr:nth-child(47) td:nth-child(1) {background-color: #6BA81E;}
    table.coverage tr:nth-child(47) td:nth-child(5) {background-color: #6BA81E;}
    table.coverage tr:nth-child(47) td:nth-child(7) {background-color: #6BA81E;}
    table.coverage tr:nth-child(48) td:nth-child(1) {background-color: #6BA81E;}
    table.coverage tr:nth-child(48) td:nth-child(5) {background-color: #6BA81E;}
    table.coverage tr:nth-child(49) td:nth-child(1) {background-color: #6BA81E;}
    table.coverage tr:nth-child(49) td:nth-child(3) {background-color: #6BA81E;}
    table.coverage tr:nth-child(49) td:nth-child(5) {background-color: #6BA81E;}
    table.coverage tr:nth-child(50) td:nth-child(1) {background-color: #6BA81E;}
    table.coverage tr:nth-child(50) td:nth-child(3) {background-color: #6BA81E;}
    table.coverage tr:nth-child(51) td:nth-child(1) {background-color: #6BA81E;}
    table.coverage tr:nth-child(51) td:nth-child(3) {background-color: #6BA81E;}
    table.coverage tr:nth-child(51) td:nth-child(5) {background-color: #6BA81E;}
    table.coverage tr:nth-child(52) td:nth-child(1) {background-color: #6BA81E;}
    table.coverage tr:nth-child(52) td:nth-child(3) {background-color: #6BA81E;}
    table.coverage tr:nth-child(52) td:nth-child(5) {background-color: #6BA81E;}
    table.coverage tr:nth-child(53) td:nth-child(1) {background-color: #6BA81E;}
    table.coverage tr:nth-child(53) td:nth-child(3) {background-color: #6BA81E;}
    table.coverage tr:nth-child(53) td:nth-child(5) {background-color: #6BA81E;}
    table.coverage tr:nth-child(54) td:nth-child(1) {background-color: #6BA81E;}
    table.coverage tr:nth-child(54) td:nth-child(5) {background-color: #6BA81E;}
    table.coverage tr:nth-child(54) td:nth-child(7) {background-color: #6BA81E;}
    table.coverage tr:nth-child(55) td:nth-child(1) {background-color: #6BA81E;}
    table.coverage tr:nth-child(55) td:nth-child(2) {background-color: #6BA81E;}
    table.coverage tr:nth-child(55) td:nth-child(5) {background-color: #6BA81E;}
    table.coverage tr:nth-child(55) td:nth-child(7) {background-color: #6BA81E;}
    table.coverage tr:nth-child(56) td:nth-child(1) {background-color: #6BA81E;}
    table.coverage tr:nth-child(56) td:nth-child(2) {background-color: #6BA81E;}
    table.coverage tr:nth-child(56) td:nth-child(5) {background-color: #6BA81E;}
    table.coverage tr:nth-child(56) td:nth-child(7) {background-color: #6BA81E;}
    table.coverage tr:nth-child(57) td:nth-child(1) {background-color: #6BA81E;}
    table.coverage tr:nth-child(57) td:nth-child(3) {background-color: #6BA81E;}
    table.coverage tr:nth-child(57) td:nth-child(5) {background-color: #6BA81E;}
    table.coverage tr:nth-child(57) td:nth-child(7) {background-color: #6BA81E;}
    table.coverage tr:nth-child(58) td:nth-child(1) {background-color: #6BA81E;}
    table.coverage tr:nth-child(58) td:nth-child(2) {background-color: #6BA81E;}
    table.coverage tr:nth-child(58) td:nth-child(3) {background-color: #6BA81E;}
    table.coverage tr:nth-child(58) td:nth-child(5) {background-color: #6BA81E;}
    table.coverage tr:nth-child(58) td:nth-child(7) {background-color: #6BA81E;}
    table.coverage tr:nth-child(59) td:nth-child(1) {background-color: #6BA81E;}
    table.coverage tr:nth-child(59) td:nth-child(2) {background-color: #6BA81E;}
    table.coverage tr:nth-child(59) td:nth-child(3) {background-color: #6BA81E;}
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
    table.coverage tr:nth-child(61) td:nth-child(7) {background-color: #6BA81E;}
    table.coverage tr:nth-child(62) td:nth-child(1) {background-color: #6BA81E;}
    table.coverage tr:nth-child(62) td:nth-child(2) {background-color: #6BA81E;}
    table.coverage tr:nth-child(62) td:nth-child(3) {background-color: #6BA81E;}
    table.coverage tr:nth-child(62) td:nth-child(7) {background-color: #6BA81E;}
    table.coverage tr:nth-child(63) td:nth-child(2) {background-color: #6BA81E;}
    table.coverage tr:nth-child(63) td:nth-child(7) {background-color: #6BA81E;}
    table.coverage tr:nth-child(64) td:nth-child(1) {background-color: #6BA81E;}
    table.coverage tr:nth-child(64) td:nth-child(5) {background-color: #6BA81E;}
    table.coverage tr:nth-child(64) td:nth-child(7) {background-color: #6BA81E;}
    table.coverage tr:nth-child(65) td:nth-child(2) {background-color: #6BA81E;}
    table.coverage tr:nth-child(65) td:nth-child(3) {background-color: #6BA81E;}
    table.coverage tr:nth-child(65) td:nth-child(5) {background-color: #6BA81E;}
    table.coverage tr:nth-child(65) td:nth-child(7) {background-color: #6BA81E;}
    table.coverage tr:nth-child(66) td:nth-child(1) {background-color: #6BA81E;}
    table.coverage tr:nth-child(66) td:nth-child(5) {background-color: #6BA81E;}
    table.coverage tr:nth-child(66) td:nth-child(7) {background-color: #6BA81E;}
    table.coverage tr:nth-child(67) td:nth-child(1) {background-color: #6BA81E;}
    table.coverage tr:nth-child(67) td:nth-child(5) {background-color: #6BA81E;}
    table.coverage tr:nth-child(68) td:nth-child(1) {background-color: #6BA81E;}
    table.coverage tr:nth-child(68) td:nth-child(3) {background-color: #6BA81E;}
    table.coverage tr:nth-child(68) td:nth-child(5) {background-color: #6BA81E;}
    table.coverage tr:nth-child(68) td:nth-child(7) {background-color: #6BA81E;}
    table.coverage tr:nth-child(69) td:nth-child(1) {background-color: #6BA81E;}
    table.coverage tr:nth-child(69) td:nth-child(5) {background-color: #6BA81E;}
    table.coverage tr:nth-child(69) td:nth-child(7) {background-color: #6BA81E;}
    table.coverage tr:nth-child(70) td:nth-child(1) {background-color: #6BA81E;}
    table.coverage tr:nth-child(70) td:nth-child(3) {background-color: #6BA81E;}
    table.coverage tr:nth-child(70) td:nth-child(7) {background-color: #6BA81E;}
    table.coverage tr:nth-child(71) td:nth-child(1) {background-color: #6BA81E;}
    table.coverage tr:nth-child(71) td:nth-child(3) {background-color: #6BA81E;}
    table.coverage tr:nth-child(71) td:nth-child(5) {background-color: #6BA81E;}
    table.coverage tr:nth-child(71) td:nth-child(7) {background-color: #6BA81E;}
    table.coverage tr:nth-child(72) td:nth-child(1) {background-color: #6BA81E;}
    table.coverage tr:nth-child(72) td:nth-child(3) {background-color: #6BA81E;}
    table.coverage tr:nth-child(72) td:nth-child(5) {background-color: #6BA81E;}
    table.coverage tr:nth-child(73) td:nth-child(1) {background-color: #6BA81E;}
    table.coverage tr:nth-child(73) td:nth-child(5) {background-color: #6BA81E;}
    table.coverage tr:nth-child(73) td:nth-child(7) {background-color: #6BA81E;}
    table.coverage tr:nth-child(74) td:nth-child(3) {background-color: #6BA81E;}
    table.coverage tr:nth-child(74) td:nth-child(5) {background-color: #6BA81E;}
    table.coverage tr:nth-child(74) td:nth-child(7) {background-color: #6BA81E;}
    table.coverage tr:nth-child(75) td:nth-child(3) {background-color: #6BA81E;}
    table.coverage tr:nth-child(75) td:nth-child(5) {background-color: #6BA81E;}
    table.coverage tr:nth-child(75) td:nth-child(7) {background-color: #6BA81E;}
    table.coverage tr:nth-child(76) td:nth-child(3) {background-color: #6BA81E;}
    table.coverage tr:nth-child(76) td:nth-child(5) {background-color: #6BA81E;}
    table.coverage tr:nth-child(77) td:nth-child(1) {background-color: #6BA81E;}
    table.coverage tr:nth-child(77) td:nth-child(3) {background-color: #6BA81E;}
    table.coverage tr:nth-child(78) td:nth-child(1) {background-color: #6BA81E;}
    table.coverage tr:nth-child(78) td:nth-child(3) {background-color: #6BA81E;}
    </style>

.. table::
    :widths: auto
    :class: coverage

    ========================================  ========================================  ========================================  ========================================  ========================================  ==  ========================================  ==  ========================================
    Scalar Functions                                                                                                                                                                                                      Aggregate Functions                           Window Functions
    ================================================================================================================================================================================================================  ==  ========================================  ==  ========================================
    :func:`abs`                               :func:`date_diff`                         :func:`is_finite`                         :func:`regexp_extract`                    st_overlaps                                   :func:`approx_distinct`                       :func:`cume_dist`
    :func:`acos`                              :func:`date_format`                       :func:`is_infinite`                       :func:`regexp_extract_all`                st_point                                      :func:`approx_most_frequent`                  :func:`dense_rank`
    :func:`all_match`                         :func:`date_parse`                        :func:`is_json_scalar`                    :func:`regexp_like`                       st_pointn                                     :func:`approx_percentile`                     :func:`first_value`
    :func:`any_keys_match`                    :func:`date_trunc`                        :func:`is_nan`                            :func:`regexp_replace`                    st_points                                     :func:`approx_set`                            :func:`lag`
    :func:`any_match`                         :func:`day`                               is_subnet_of                              regexp_split                              st_polygon                                    :func:`arbitrary`                             :func:`last_value`
    :func:`any_values_match`                  :func:`day_of_month`                      jaccard_index                             regress                                   st_relate                                     :func:`array_agg`                             :func:`lead`
    :func:`array_average`                     :func:`day_of_week`                       :func:`json_array_contains`               reidentification_potential                st_startpoint                                 :func:`avg`                                   :func:`nth_value`
    array_cum_sum                             :func:`day_of_year`                       json_array_get                            :func:`remove_nulls`                      st_symdifference                              :func:`bitwise_and_agg`                       :func:`ntile`
    :func:`array_distinct`                    :func:`degrees`                           :func:`json_array_length`                 render                                    st_touches                                    :func:`bitwise_or_agg`                        :func:`percent_rank`
    :func:`array_duplicates`                  :func:`dow`                               :func:`json_extract`                      :func:`repeat`                            st_union                                      :func:`bool_and`                              :func:`rank`
    :func:`array_except`                      :func:`doy`                               :func:`json_extract_scalar`               :func:`replace`                           st_within                                     :func:`bool_or`                               :func:`row_number`
    :func:`array_frequency`                   :func:`e`                                 :func:`json_format`                       replace_first                             st_x                                          :func:`checksum`
    :func:`array_has_duplicates`              :func:`element_at`                        :func:`json_parse`                        :func:`reverse`                           st_xmax                                       classification_fall_out
    :func:`array_intersect`                   :func:`empty_approx_set`                  :func:`json_size`                         rgb                                       st_xmin                                       classification_miss_rate
    :func:`array_join`                        :func:`ends_with`                         key_sampling_percent                      :func:`round`                             st_y                                          classification_precision
    array_least_frequent                      enum_key                                  :func:`laplace_cdf`                       :func:`rpad`                              st_ymax                                       classification_recall
    :func:`array_max`                         :func:`exp`                               :func:`last_day_of_month`                 :func:`rtrim`                             st_ymin                                       classification_thresholds
    array_max_by                              expand_envelope                           :func:`least`                             scale_qdigest                             :func:`starts_with`                           convex_hull_agg
    :func:`array_min`                         :func:`f_cdf`                             :func:`length`                            :func:`second`                            :func:`strpos`                                :func:`corr`
    array_min_by                              features                                  :func:`levenshtein_distance`              secure_rand                               :func:`strrpos`                               :func:`count`
    :func:`array_normalize`                   :func:`filter`                            line_interpolate_point                    secure_random                             :func:`substr`                                :func:`count_if`
    :func:`array_position`                    :func:`filter`                            line_locate_point                         :func:`sequence`                          :func:`tan`                                   :func:`covar_pop`
    :func:`array_remove`                      :func:`find_first`                        :func:`ln`                                :func:`sha1`                              :func:`tanh`                                  :func:`covar_samp`
    :func:`array_sort`                        :func:`find_first_index`                  localtime                                 :func:`sha256`                            tdigest_agg                                   differential_entropy
    :func:`array_sort_desc`                   :func:`flatten`                           localtimestamp                            :func:`sha512`                            :func:`timezone_hour`                         :func:`entropy`
    :func:`array_sum`                         flatten_geometry_collections              :func:`log10`                             :func:`shuffle`                           :func:`timezone_minute`                       evaluate_classifier_predictions
    array_top_n                               :func:`floor`                             :func:`log2`                              :func:`sign`                              :func:`to_base`                               :func:`every`
    :func:`array_union`                       fnv1_32                                   :func:`lower`                             simplify_geometry                         to_base32                                     :func:`geometric_mean`
    :func:`arrays_overlap`                    fnv1_64                                   :func:`lpad`                              :func:`sin`                               :func:`to_base64`                             geometry_union_agg
    :func:`asin`                              fnv1a_32                                  :func:`ltrim`                             :func:`slice`                             :func:`to_base64url`                          :func:`histogram`
    :func:`atan`                              fnv1a_64                                  :func:`map`                               spatial_partitions                        :func:`to_big_endian_32`                      khyperloglog_agg
    :func:`atan2`                             :func:`format_datetime`                   :func:`map_concat`                        :func:`split`                             :func:`to_big_endian_64`                      :func:`kurtosis`
    bar                                       :func:`from_base`                         :func:`map_entries`                       :func:`split_part`                        to_geometry                                   learn_classifier
    :func:`beta_cdf`                          from_base32                               :func:`map_filter`                        :func:`split_to_map`                      :func:`to_hex`                                learn_libsvm_classifier
    bing_tile                                 :func:`from_base64`                       :func:`map_from_entries`                  split_to_multimap                         :func:`to_ieee754_32`                         learn_libsvm_regressor
    bing_tile_at                              :func:`from_base64url`                    :func:`map_keys`                          :func:`spooky_hash_v2_32`                 :func:`to_ieee754_64`                         learn_regressor
    bing_tile_children                        :func:`from_big_endian_32`                map_keys_by_top_n_values                  :func:`spooky_hash_v2_64`                 to_iso8601                                    make_set_digest
    bing_tile_coordinates                     :func:`from_big_endian_64`                :func:`map_normalize`                     :func:`sqrt`                              to_milliseconds                               :func:`map_agg`
    bing_tile_parent                          :func:`from_hex`                          map_remove_null_values                    st_area                                   to_spherical_geography                        :func:`map_union`
    bing_tile_polygon                         :func:`from_ieee754_32`                   :func:`map_subset`                        st_asbinary                               :func:`to_unixtime`                           :func:`map_union_sum`
    bing_tile_quadkey                         :func:`from_ieee754_64`                   :func:`map_top_n`                         st_astext                                 :func:`to_utf8`                               :func:`max`
    bing_tile_zoom_level                      :func:`from_iso8601_date`                 map_top_n_keys                            st_boundary                               trail                                         :func:`max_by`
    bing_tiles_around                         from_iso8601_timestamp                    map_top_n_values                          st_buffer                                 :func:`transform`                             :func:`merge`
    :func:`binomial_cdf`                      :func:`from_unixtime`                     :func:`map_values`                        st_centroid                               :func:`transform_keys`                        merge_set_digest
    :func:`bit_count`                         :func:`from_utf8`                         :func:`map_zip_with`                      st_contains                               :func:`transform_values`                      :func:`min`
    :func:`bitwise_and`                       :func:`gamma_cdf`                         :func:`md5`                               st_convexhull                             :func:`trim`                                  :func:`min_by`
    :func:`bitwise_arithmetic_shift_right`    geometry_as_geojson                       merge_hll                                 st_coorddim                               :func:`trim_array`                            :func:`multimap_agg`
    :func:`bitwise_left_shift`                geometry_from_geojson                     merge_khll                                st_crosses                                :func:`truncate`                              noisy_avg_gaussian
    :func:`bitwise_logical_shift_right`       geometry_invalid_reason                   :func:`millisecond`                       st_difference                             :func:`typeof`                                noisy_count_gaussian
    :func:`bitwise_not`                       geometry_nearest_points                   :func:`minute`                            st_dimension                              uniqueness_distribution                       noisy_count_if_gaussian
    :func:`bitwise_or`                        geometry_to_bing_tiles                    :func:`mod`                               st_disjoint                               :func:`upper`                                 noisy_sum_gaussian
    :func:`bitwise_right_shift`               geometry_to_dissolved_bing_tiles          :func:`month`                             st_distance                               :func:`url_decode`                            numeric_histogram
    :func:`bitwise_right_shift_arithmetic`    geometry_union                            :func:`multimap_from_entries`             st_endpoint                               :func:`url_encode`                            qdigest_agg
    :func:`bitwise_shift_left`                great_circle_distance                     murmur3_x64_128                           st_envelope                               :func:`url_extract_fragment`                  :func:`reduce_agg`
    :func:`bitwise_xor`                       :func:`greatest`                          myanmar_font_encoding                     st_envelopeaspts                          :func:`url_extract_host`                      :func:`regr_avgx`
    :func:`cardinality`                       :func:`hamming_distance`                  myanmar_normalize_unicode                 st_equals                                 :func:`url_extract_parameter`                 :func:`regr_avgy`
    :func:`cauchy_cdf`                        hash_counts                               :func:`nan`                               st_exteriorring                           :func:`url_extract_path`                      :func:`regr_count`
    :func:`cbrt`                              :func:`hmac_md5`                          :func:`ngrams`                            st_geometries                             :func:`url_extract_port`                      :func:`regr_intercept`
    :func:`ceil`                              :func:`hmac_sha1`                         :func:`no_keys_match`                     st_geometryfromtext                       :func:`url_extract_protocol`                  :func:`regr_r2`
    :func:`ceiling`                           :func:`hmac_sha256`                       :func:`no_values_match`                   st_geometryn                              :func:`url_extract_query`                     :func:`regr_slope`
    :func:`chi_squared_cdf`                   :func:`hmac_sha512`                       :func:`none_match`                        st_geometrytype                           uuid                                          :func:`regr_sxx`
    :func:`chr`                               :func:`hour`                              :func:`normal_cdf`                        st_geomfrombinary                         value_at_quantile                             :func:`regr_sxy`
    classify                                  :func:`infinity`                          normalize                                 st_interiorringn                          values_at_quantiles                           :func:`regr_syy`
    :func:`codepoint`                         intersection_cardinality                  now                                       st_interiorrings                          :func:`week`                                  :func:`set_agg`
    color                                     :func:`inverse_beta_cdf`                  :func:`parse_datetime`                    st_intersection                           :func:`week_of_year`                          :func:`set_union`
    :func:`combinations`                      inverse_binomial_cdf                      parse_duration                            st_intersects                             :func:`weibull_cdf`                           :func:`skewness`
    :func:`concat`                            inverse_cauchy_cdf                        parse_presto_data_size                    st_isclosed                               :func:`width_bucket`                          spatial_partitioning
    :func:`contains`                          inverse_chi_squared_cdf                   :func:`pi`                                st_isempty                                :func:`wilson_interval_lower`                 :func:`stddev`
    :func:`cos`                               inverse_f_cdf                             pinot_binary_decimal_to_double            st_isring                                 :func:`wilson_interval_upper`                 :func:`stddev_pop`
    :func:`cosh`                              inverse_gamma_cdf                         :func:`poisson_cdf`                       st_issimple                               word_stem                                     :func:`stddev_samp`
    :func:`cosine_similarity`                 inverse_laplace_cdf                       :func:`pow`                               st_isvalid                                :func:`xxhash64`                              :func:`sum`
    :func:`crc32`                             inverse_normal_cdf                        :func:`power`                             st_length                                 :func:`year`                                  tdigest_agg
    :func:`current_date`                      inverse_poisson_cdf                       quantile_at_value                         st_linefromtext                           :func:`year_of_week`                          :func:`var_pop`
    current_time                              inverse_weibull_cdf                       :func:`quarter`                           st_linestring                             :func:`yow`                                   :func:`var_samp`
    current_timestamp                         ip_prefix                                 :func:`radians`                           st_multipoint                             :func:`zip`                                   :func:`variance`
    current_timezone                          ip_subnet_max                             :func:`rand`                              st_numgeometries                          :func:`zip_with`
    :func:`date`                              ip_subnet_min                             :func:`random`                            st_numinteriorring
    :func:`date_add`                          ip_subnet_range                           :func:`reduce`                            st_numpoints
    ========================================  ========================================  ========================================  ========================================  ========================================  ==  ========================================  ==  ========================================
