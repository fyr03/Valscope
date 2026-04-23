CREATE TABLE subset3_ebeb1651 (
    c1 INT NOT NULL AUTO_INCREMENT,
    c2 VARCHAR(255) NOT NULL,
    c3 VARCHAR(255) NULL,
    c4 INT NULL,
    c5 DATE NOT NULL,
    c6 VARCHAR(10) NOT NULL,
    PRIMARY KEY (c1)
);;
CREATE TABLE subset3_ref_ebeb1651_t3 (
    c1 INT NOT NULL AUTO_INCREMENT,
    c2 INT NOT NULL,
    c3 INT NOT NULL,
    c4 YEAR NOT NULL,
    c5 TIME NULL,
    c6 TINYINT NULL,
    c7 SMALLINT NULL,
    c8 MEDIUMINT NULL,
    c9 BIGINT NULL,
    c10 LONGTEXT NULL,
    c11 GEOMETRY NULL,
    c12 TINYTEXT NULL,
    c13 TINYBLOB NULL,
    c14 SET('x','y','z') NULL,
    c15 TINYINT(1) NULL,
    PRIMARY KEY (c1)
);;
CREATE INDEX i_s3_ebeb1651 ON subset3_ebeb1651 (`c4`);
CREATE INDEX i_s3_ebeb1651_c5 ON subset3_ebeb1651 (`c5`);
CREATE INDEX i_s3_ebeb1651_c6 ON subset3_ebeb1651 (`c6`(64));
CREATE INDEX i_s3_ebeb1651_comp ON subset3_ebeb1651 (`c4`, `c5`);
INSERT IGNORE INTO subset3_ebeb1651 (`c1`, `c2`, `c3`, `c4`, `c5`, `c6`) VALUES (7046388, '2023-01-01', '1', -6, 'not-a-date', 'ipsnbzc55-dckk2sr');
INSERT IGNORE INTO subset3_ebeb1651 (`c1`, `c2`, `c3`, `c4`, `c5`, `c6`) VALUES (540004, '2-:on13trqzkvq', 'not-a-date', -6, '2023-01-01', '2023-01-01');
INSERT IGNORE INTO subset3_ebeb1651 (`c1`, `c2`, `c3`, `c4`, `c5`, `c6`) VALUES (6842882, 'hv_7711', '0000-00-00', -6, '1990-01-02', 'hv_4224');
INSERT IGNORE INTO subset3_ebeb1651 (`c1`, `c2`, `c3`, `c4`, `c5`, `c6`) VALUES (2607945, '0', 'ngq8uupo_', -10, '2012-12-12', ':vgvr6-2j584:fx_');
INSERT IGNORE INTO subset3_ebeb1651 (`c1`, `c2`, `c3`, `c4`, `c5`, `c6`) VALUES (249319, 'hv_7711', '0', 319, '1995-09-09', 's1nonkracqa2:_a3');
INSERT IGNORE INTO subset3_ebeb1651 (`c1`, `c2`, `c3`, `c4`, `c5`, `c6`) VALUES (5799570, 'il rov:f:', '-xt2o:7i 0t83wbcqe7l', 425, '2002-03-25', '2023-01-01');
INSERT IGNORE INTO subset3_ebeb1651 (`c1`, `c2`, `c3`, `c4`, `c5`, `c6`) VALUES (4770872, '56jzm:3qm3tj', 'hv_4652', -10, 'not-a-date', 'hg4lmnzf79');
INSERT IGNORE INTO subset3_ebeb1651 (`c1`, `c2`, `c3`, `c4`, `c5`, `c6`) VALUES (5072138, 'hv_7711', 'a1ddap10uqmfn0v8cs9', -10, '2030-01-09', 'xg-1vxgk:ye9h');
INSERT IGNORE INTO subset3_ebeb1651 (`c1`, `c2`, `c3`, `c4`, `c5`, `c6`) VALUES (446419, NULL, '0', NULL, NULL, NULL);
INSERT IGNORE INTO subset3_ebeb1651 (`c1`, `c2`, `c3`, `c4`, `c5`, `c6`) VALUES (3562502, 'not-a-date', NULL, NULL, NULL, NULL);
INSERT IGNORE INTO subset3_ebeb1651 (`c1`, `c2`, `c3`, `c4`, `c5`, `c6`) VALUES (9192654, NULL, '_', NULL, NULL, NULL);
INSERT IGNORE INTO subset3_ebeb1651 (`c1`, `c2`, `c3`, `c4`, `c5`, `c6`) VALUES (9762534, NULL, NULL, 0, NULL, NULL);
INSERT IGNORE INTO subset3_ref_ebeb1651_t3 (c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12, c13, c14, c15) VALUES (1, 19, 3, 2005, '2023-01-01 12:02:00', 50, 55, 50, 84, 'sample_qrRrwVjZvrIQcc0nNmL0YTXaAb5dmLDSgbRyp4DJjlwE3SMFARZFCgzevkmki4UX20HCOSnpL3mOyn8YchWGBTecYhTTfqR2ZUvcZqFFIbg3ZiCpfoV7BkhVroNy8yYIOr3wN8PPhoDPbRc3t1hPrLrJeZdxWdEOz9Pi5ilbHgc7XNxLiMTGphxfLocITbvLwx8ys2jDW46DzmbXFcAIn7Vehbc2athHKafMDG7LxaUp8n8QUdIQWh8Swz1yBymw5sLmwRnrvl5fhtPwmGIigGmQum4TlusQPecxGbuHWbqgIKqr7iNit4TiCDWhOJxSkdv30vZ7mP19I87cWsWAhWvDA6UweLLXalcAa7Ab9ISdKSIiMji3sDwKHcfKENjw4lqaUZF5hzF51W4nL4bGGoqHLTovdkCEXcHv9L4Uy71w6uPxCUkDl03UdU9ixVbw9jEBfrLxwP2lvhwQgFFAqOG1Khcw8VPr4reEtTcw4xeepuS9SBUavA0A7D9k9hnG0qw50PXTQmXo0F4qulEi5Wez7UB2TMkxrwZZUygOGvnhXzH8A6xiS9JNAaD9rRyVtoIFCPLHpPh88VJymOOlS7uFreuXqgpqt5GlYLfFDPq063Omii1Es22qmLqtFlkibZRtl6S0vGx1pVpLAEBj6tWZbFpjvGGRSjT7okiwFhqwQ5JyO5dotzUlYbBEMevB9iPPLmCTyJZJw5lV9T3IwhwR8VceISoSPHULeAiGgOxpBGKbNsBWaI8kr82uHpmQCWbwGfvxP7Py7kcaEr2Ma3WrygKZOrKkxYeS7c8in1nDG22r1sJwkinp3DHauK5MSCgfJveFId5YfILPpfqy60DwLBScFhOXWRcnR7WyiFiVE9uJp9jbJBddVlZSjBrVHpTUIzhatgesG5QAfO29kDUgA6NYhIvXFbhdDj9ouRXIuQ7ykFcDgm9G4UxY8osLTZpRSb1vKkuymzDIM86thH4nq7SONJmz2e3Jxia1cdHt391DsJX4rst2BM0pwhHKGy2cGIMm1PZUkMHElIYeHiXkoJXAv7YBGlMrVruGv2VwixenwNFw3Nydly74RIhHmp0ZGOdX1xMhibj1mHIsqa9BAzCEH7AWnvWI64eVq8es1pkNprj09UvxIqzkvWS0FmZE5OkI0537', ST_GeomFromText('POINT(-59.478542 151.108953)'), 'sample_NdJ1uJhUQIcKxpOUhjMFI1jjl3NptkfFIy7WpDnKiXYts1KGWWi5yJiLldHf9UfwniM82VyKvO3ERtNqsR191Dir09UVjGbFNqvx6xlz5ETyR48mutXI8A8Z8KPqPyLqUtOWimkKR9IZK9hetWtWmiDlaDxJ0k3APeBV1pFnhqDfd', X'567E4172CA984A', 'x', NULL);
INSERT IGNORE INTO subset3_ref_ebeb1651_t3 (c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12, c13, c14, c15) VALUES (2, 16, 4, 2020, '2023-01-01 23:42:44', 9, 53, 32, 70, 'sample_UMO08d4MrPn0unYQlYX12PvFZ8Br3Jf4XmTq7u25pHbhw2QfrDRQx1O4X8q68U4qAdXnppfAzpQoEr43cLc7RPxg08zcdll1ypdE4TFBbcQVeeDZTFXV0xydhlGko8Seiz6n1W0NXa2Odp7P7oSi9h3bhlskkReqgjOVECRKuAL9OLPtngertsQMGFhJzJZhOQzu1pWYcB4AKmeKBU2JyhlHM6EFVKhu8aiRlGrQeoFufD17ybIJkSDOocKWNmgw5ALqjn0khJEk5H9hHIaQJGJjw22lAQvoRrru2C0KgR49RKXBFfP51MsGQTm9VK6Hrfl3lZCjxpq45YS9m6NVzTX4aeaMoRFM6eUZvlN6EDMrzwa8SKxlmjQw2Jk0ovWrVZdOlEFp5U2Mf25ojLbdoNojFY9yyQFToKSNynTqtfdSPYaH9HCdSArivZUFF8KzTJ1MleaLJzagd8gymlRR2bLLaE7NCcehyLVHATYqF7HBnyCqT8TBAArjnSHF9TuTALm95Aa2pQ6jMopXZbSRVtestqEcRbUhwjluwaLw120hnSp05FBL3hjWkhuI7zzgHrmN42umF5pnOXI7deC98L5uIbaSmUURxWq98dI6PYqeB6mhm5AMHY1ByKuW0OnxIkx3R9OLei46WrmJkrLh52zT8Tzz3Mkw5TnXoYFUe2AeGPA2buQb3pfItbOZn5TUYDnfTGElkbupw6nGVL8jBYPAm5WvzHdxGvD0eupyp37WxzQuXWcaFD67S5LRxt5ak2179tKMnyXEVwlgDRUhx7FAdUa9C6YkudsyM5qAiemxKVCQResUUw6EnBeIbEItDbcd1bB23nDJbz5ZuEgfgx0J3jk1HCKGsmWbB1DJfrgsSpLmLsGun6UoFUrJnHnK7dcrxjIeiWVUxNzBA0E0hKVpkT9XDrETlFfjUx3FYUOIHMts7Tru9j4svHTYXjUS6HInXhuZyUKGAKZAHX1LTGPy9r8n6CIW6olMYqoMJtmCJNfjS77tCEoHfLUCyWPUj41nIWDKEz6PW63z2lAkQCvDt3LsHLEesXRSbXt6pkV2YlToB79swydVkARzYsIStxzbTFXiwy83ZIosMEcf5nVbbSeNxTGNN0x9ePUOonDwcQ5U36Mdmcgl86G9LPsTA6Y2bUafDgKoWlxQMXqmEvaQ9q4red6k9m6cwCr88TPnfksnkpA46jpjOTmJ0EbluQ5fuMxVaSQvzbzylysEr3arUMMw5gu3EAVzbvYcmSnlXnF1FB71OOG05UOdfWeI9HYgcdGbcj4jTygqZWz1TPYxsRoS80JLSIl1JgBKyjds5wrDQC4Ye35HDc2DqyI87ntWDA2FEVJ3b8O5LqGV9Te1aSmXG1KbpK4kFXdvzL36cvKLuhHmV9ML8K4QbgKXdh5Yqoi0dSauwfXXeq3S1pCRAdxkVjFqjCnN6z8igHIOHdTzyjJOmYdUMlDRperl4iTe9VBReQsVUIJANklrP3BMJx8JIJDstMMk1YRfDEdrtGk37FntqVWwCG6xQBqFgiPJzO0GsmWgZvEPzslKJ5ejrA6fkpA2Uc2CephC1mqZw89UNITZTMLbIWzrSS7nqqdMftu3Er4WJ3jlTTJo3RQGPNsPKpgeSgv4F2awkQLAJpXpRjGfhL93vYOqTe8BVhHKmOgbmAwKxUjhZQopwAxQej4OCIETNOmQ8pBE5wxDn216T0ih95tbXKU2nJPLfOvVnXIFxO52cTCp3R8MB9oLXBFeXbc9nKXC', ST_GeomFromText('POINT(88.246485 62.832521)'), 'sample_cysGEpJSqKwJeYO0uop8CAKIDbmKzG63A47Nnhj0rYva2WQTEOOo0GsG92XcmHU9QoBFltJLGjLgrQYu6KkA11Deo', X'E3BC84ECB4B667E7B9BFC59BE8899BC981E1AAB016E09B93E182871AEEA1AE0BD6B975', 'y', NULL);
INSERT IGNORE INTO subset3_ref_ebeb1651_t3 (c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12, c13, c14, c15) VALUES (3, 19, 15, 2002, '2023-01-01 06:25:27', 84, 42, 38, 97, 'sample_VbcB2id9KL38RybWe8I53yRVDGDRUgWHtluidG8bmi4ROZqudMSNV4RB1jvMzR83dR6t7MT4hioW4sbvm8OkWG33xXcZy4VJTYarZw4Bj6safqwbrzcte17RD9p0Zok7MlRmpnHSV043G6kmFN3yOSlX1khPb1WUJmOrQ27tIaRyBL3QfgNV1AZ5IHOVkm8NQ9KDQRJPCY2Db1ucBSWHy6l9L6TGfUZEZA58dkufqRLFdTR2Wj9ZUK5IU5M27PjexpoQv5k6ytRF4iYxLg6IyD4g43IVigMrypjLUEpNFGlPo6CjSZVC3tAhLtMzzy1riUWR3UmX5kjv8nSBj70BKsB73SRWYgrSQadghULyVs16C1KNtbR7nK2bPB6Aiea6dAA6QK3BeIg01WjfFd9NGIM2nBdTI4fGtmmO5J40Y3xvh21YmO9yz57A44ZLImIe3RTro9hgDf1xsShZZcUtaLl21ia287nVcC7Uw3DIkW4DvZAwjj3fPWjfTeUPmHqAaBAxAuFodXxwY2mPKR0roV7sDwfmkzgGr5Lthuh5HtVOrTR9jWq1u99tXiAujAT1ZfzYE5wZzW8qvbS0ZR962TCyBNKv0ZhprXvWd7QFgPhJr68EwTM9ahtNicFkHW7uC6C288HljeGdkJN4sKUANILrnU4arY7TxSJrs6rhMu6wqWiQGq513HCJJjjg86YcyGob0g3XwBuRW3KoNatefDtlElZ4a3gUIszG6bCJ8bH9Pkb6IePnNFYD2A70TRbkhaFeelvQbZ5l2YVJkDDg63EKpv18dHKmbB2BXNWYmaAl1uPnZ0Qh7vLN2wbnuszGO4lAopAffFMn8XweKWBUPAayPrUOhPHfqVnT94eQzSmtJmg8magMjBW8hqX7NY5oTustYWqn9uMmdAPEXjvxLBp0pOqG6tl47v03ySZnsSxH1o5qwxRw4bPWPVKYC9WCZuxpx1IyqjyTvQ7SPT1OLTgNPfxSYP6GbLsDphFRwHUFwbDDVoiShfdVqCGhyYZUIEEaPvDPsJu77S5vY422gAOontkRMSVkmpZM4VR5BvyNIfM3zVn542RB9j51kdz2zZYZ7fMgBjyD37eHkbXmIwIdHJ2pvqBhUBtYCdH1eTuDKJ6t6UrbEYYYxkzXjsDbiR2M0nLH7X0JqTnh40cu1uBvQbHrildoZftVUefqLWf2a6ndr0RWsod8HSg5O1hf4AGbwMyVpJu8Pjq4UQPhgTkGGe78LbkaVWC9QRB0jNQYiiAHmYxIVVqKAvbOC3mezpvMAqFGjaj9J8qdMDKSSxg7k3n0V74MUZ7N194X0aHFGaCBXk02L1LW9JhNbhtB8yjbX4dAvgjpqAh7s80VJJRy8ismdu64igiCDDgLjmlYy1oQHXpFFNLC1OjUsC8ruRG7TKopnNCtYcXwNiujxtHUJcSei8OwSDKDs9WnmiTGDA5946ZNolYppbLQ6Gc8V4TCJjKDKZ9cqTooyYnCe6Iugm2iKYgQmFrWw7vju40ERA2HsFFpUVtFngxX4XQl7doN7J4h1OChPtKgvZ40HvigLtEbME9p5XzjB0m94t6Vqj1PHLypRZyRAx6X9xUgG8comYDQeEV7BxhOewDivPvqUIclFl7h0Zgwt0iQbRsyQIgNsP95fJ5gp4ftkNBnTN7N5JNuLzn6aHWlUpshKBaryhOc0W2MATpJJZ8ds6ioCCXgn8VfiUQJbs1c2ogTHghq33kwPTnKMqmyYRON0OwsolLrrOX6MZ5icm8', ST_GeomFromText('POINT(-58.661249 -111.570335)'), 'sample_ToavYxh2RWPDiNVtBsul3wvhTnrLVVxIOETh3DDFKFIKQZg07wEkJpslJYeEjr', X'4AC0BB36EC909DEAB59AE69FB4DA8474E1B588E9929FEB8B80DBB7EC8FBD3FC38E5EDF8FD5BB', 'y,z,x', NULL);
INSERT IGNORE INTO subset3_ref_ebeb1651_t3 (c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12, c13, c14, c15) VALUES (4, 8, 9, 2000, '2023-01-01 23:49:12', 68, 75, 57, 94, 'sample_6wvch5BWbgDXSKE9rKgZoiSUci3Bj69wQXlhmRrUiAozV1MS6gUlhdxKMlIebtUGT6zTOQdyQuBnDmGDgy5oo2xSvVVN2SR5YYqUIH74fazmflonwYS4Jueh0T64xTUVlOkQgrJdnjXQfYGBxkdtVsghTOPWRywkgbOPswhuBCl5cHJUJelD8noErLRUgNsgZjxcrqoY5o1hbup4RrBj9cspQxfliy0fhhCtgVTyMWvGy5DZHuULZslQmmeYLHqHuSj2e4NaNDDVZT9RvHOxdnrYgmV5y4ff4qhL5KDqETpeXUStcytl2X64qbT9Z6P3WBM6vRwIVOqg6AKwfkOWGHLNOCdOeCEsR5ep86M9cp8V62POAxBTJGWZvhlEj4WO3dii6qjut2JLdjIiyIpGaMmk60TthqjeJPBSd2t6Lgci3wrdEoZ24mJ3kXhBK5xRnyka9sgIxq5tj9x4ZG9vQ26fSB5r9AQjvZheTtgPHtWpT5CPG1h56zSIm4lHQDcVzEIHXMMPJgMLstcpLYkmz3Aan1V2mdPTyAT3Wk1PuyfdivM7i46U0jatuvfgtRzR1b2KzDqsWpaJPpJouRClRv5s65kevCBdG36WsBzqek3ETIFuIOnnNKN7cYmmn7e44KpHnPUwPyO2Rq4hxBEXHWHU8jbVjmIwEYRA2dXjyCd8EEYaoKiarQo9U4tSRG3uBdj4gtiUe1iZCbqYBjxrAuqX4hsTqiAmKKK7fXMsWw4o9YKDdapayZCvwjb4SzpEQ3K3gyu0uYzIgEeEg9nSEEzQZ9FBk8sIxP26LLhdN2mtzvWzb1KhQYsOU7AbwmjgeOZ1dXNfSFh5o3ldi6CXXhZtQC0brHNzl7LCPLdVHfRRPVNgm68BwTAbMDqJd2PXBajTIm3DD2y3SGXTAmEwXFyOeeyaVrhI2HaxWjFO6GdLzPcbomW48dfIL6bdS31owvGrEcj4ZBBWSSOAP', ST_GeomFromText('POINT(74.851109 -0.398653)'), 'sample_B5RT6rWlxCzFBxwTztPHW7NpVKl0Xv1zwxwSqOGavx1a88YXqFbrimcX5KTk3Jd3GVo8qpg9yeOmPllTKps8pJErKdUlgIl1WIVOeZxMQZ8F80WlPp25vW6KG1YiLjssJb9zPhTEkjV9lDyfOqkkVBltiobdvZGZJuRojDCer5VqA8pHLvwpbW', X'D099C3A5E6BC9AC3970C451EE890B95330E4B0B1D89CE499B0C39375', 'y,x', NULL);
INSERT IGNORE INTO subset3_ref_ebeb1651_t3 (c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12, c13, c14, c15) VALUES (5, 14, 15, 2002, '2023-01-01 08:45:40', 30, 41, 13, 33, 'sample_SgKXjm0VSaxcZUbU9c1VdiS6QyjqVAdgBzwehAb70FbvC6m5RtKMTxwBx6iKcxzK97Er8sLpSrAFHRvmyMS0q5HPWNfgkWpjNlRdhSCSObDId3elKvVihO7paw5ogiO72KIrvZu4ffuYT8L6JPHTqvQ1y0EaBx0YSv6WaRuDW4NNlklh73BkgUMNTgjJuqi2regP09TBxhrBdtVU9mYW5w4kyKWKyaP3JmemCES8DcGGGQ3CjgHRjrGsPhafsjklJUd4qg3YhqpO2i2fRQ5Ji1G1eRVwmUVnY16aVjm2T7WRFZPUcq7fgKFebb4bUVKq4mxxtb36NHNVjSMwfrQ0WWeigWmuPXI1Bwd4RJgQHMYCuJty5B6oL2YdTF35oDaLbNdhvk1bkntUjqDZk330AWLgoDzVub8weI2r7S7WHEclDSrlCugSwkYoCfo2g7oj1ZqQbasOwooWyUd1OGFVOr1BiTGWya22Y3hfWIW7cGeQwRXdMCUjRSC1CzrIpkb8LfrLQf89CEB8OJ7ABTMCohOeCfRXhBNm8agMLjYgpvPtdk8pZbZ1SJuC8mMiHDSzgQoY10i5eBZaFMwZi2Xts0NlHcKAG6DoThbKXzVzAgV9cI2l3ZZFrAFF74e9GRiHsr9Ct7gzemzXF9eyXWj6T9LxHGhQ2p2qmB6RsDY0l0INWB3qX4NUGG9EKByHOC1ZHQG4S1dm8UJSVSWcnKkyqrQigdowUSqFs0ZzFllw4iPYaxHuGH1JW5UUadlTAz93VGShOSTx084TNtMP0w90xUeXvrlaQ3f0026hzTvqM8cpeNeZQHNkrN7VDHepEEZK72GW4uDYTHBN0k7k1Hs8JGQCkD0BYjrbi5jEb79VYttFdhQFFq6JK80FSAN7IMcva52519W6KQSDwqp3xjkalCzjekxJCedhjLdLZlCmMwrQRx23HK86Kj7l7zoa8G0UB5G4vC46hin4F8iKcqoIfEy6PTZ67bohePB2N2bZoSrP3syhUDtGMEbE6j8MXQVixjjUC5o020dx604DBEAmSTrz2xiGpy816SAlnI0RLp3gcnryaK7hYUEECrZ3nJlzHNrnX0b8zAiu66PU8WXAfFOtmaA3yaZrWcufESM3xhJP2F2yQ9qZHtnY7FwkZkEzIt74evOmObvC4URG2mLbJjJ1freQlnUJKrhblegNy7zLidNuwr2hlzMBYTQRH43gT8sXuVkAPkvZHFnedcXGQEkCdOZZ4YzFfIumABUFxKBWk5H5cPan8MxcYaYzXYRHi8SeSA528EBbkR9g4Dqt1sjYYx7G1nwdXDLdGmlqaby0byqwrwx38FKSYRGL96Tn6qNH3GkjL1IKPJw7htiZS3VisT7309ZMHyL5EduyeC0D5ZrqjY8z009FDms5SYXLeGoyxxf7Uo3yeITOpz6vEIpmicxf4PJlwPgnEtLfVK9Pveq', ST_GeomFromText('POINT(8.866695 -28.157721)'), 'sample_ODsQ9j2uGI1wehnWMbxu4dgdJSHaKaMCIpvc4qe4UTYsQODamGEb05ertIJqHMJPMvknuUklbR4DR3spPw7erMoiv1WwyUHTvtcX9c1mvKg2zwDyZruShf6SGejZ62ckl0s1c9bFcCXfmBUuemk1W6zpieFr', X'0AE0BEB419E5BC8ECDB41BE797A0', 'z', NULL);
INSERT IGNORE INTO subset3_ref_ebeb1651_t3 (c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12, c13, c14, c15) VALUES (6, 14, 18, 2019, '2023-01-01 15:33:00', 98, 36, 2, 50, 'sample_YnJYq0jY1FoALYH1OZiUhldt0QhPdxYXQNRQDYWVOX2cijZZHxFBPTpfGJ0GlxJnvqYFh5D60biacZxNYuEPmEPLD0Kj5vhvhZnFr7oBMb8srgXktikbOYdQyykIm9ZgPtQi5yQAS7SlfgFkzchzWzsXjKd7J4sxRJAECZRZIVitcXl57vAkLrwVkvagxysnM9giJpzmadwS1eFftdna06M9MRTlqazWzDzgbutfLqturXyTBWFuRNxmXHgB3lAeTgmbstVAyut9neghLNxDPl32rdZNRdYXNoIFtp1anRmeYnptnagY66bXx2WZFL7lQRoH1CXsZals1pNaEMHfLWZbSvPwzlsxul8ljcLK8zqrvelBVPc7WlgjYhZ9qiPACwOAPXZ53kPNoZWvPDEXhltpSF0oyz225QglZHpUDSaLUV3x38zA7sVwe6ohwZX91hMXhqi7AWT8PLB73mrjpWgHuov4PBnosSRdq6aA6qHq3NPn8QFAfPnlpxt30JF0bGnmWU5GLULql4Q2jS5S4DNDzNTNxcrAAPZ0XjsO6c7QgFKErxHg9o7kqwRS3nXHlKnOKX8pewJUJZEgEQNi6lSCSQLkkdQ1B2G2o4hUCeXP1tqun7ivcUPVhCiXA01oYdozv79OJL2gmQhmJp6uX3C7lCrxNbxTvjsyGS6b4PGSZgyRMbV4MBUGJwuOKH8eSYYA4XcBSexGosAAbsEycQF79l5UNk3jR4wIDQpBydDwwj0Pd4hOJLImJJoU8kgvzMAgPQJNZsKI8SSSd4xFIZGIdxDvxZmIGWgqvjEjGu1gkx1IBZibWq1oJsEmcAc7bjyCQQQ', ST_GeomFromText('POINT(56.718617 -179.862261)'), 'sample_lRrITYN22UJkaUKSoUlW74QIizM4OnuyK9PSbeixaZzYcnj2eG6JnzyKrOhksEB6x', X'663DE989A8E9AF9524D7BA62C58FD7A6D580E685A92FDF82C289E2B9BED5932666DCA8C1BCEB82A8E3A590C396D9AD53', 'z,y', NULL);
INSERT IGNORE INTO subset3_ref_ebeb1651_t3 (c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12, c13, c14, c15) VALUES (7, 5, 15, 2017, '2023-01-01 17:01:22', 97, 56, 93, 75, 'sample_8HV8CpfJY1m67iOtZ5XiOW0sM02sdSVsxKlWFPK6du9uf3ieSZI4y7RmH5fLNloUw46Nm4RdmM5C4SHwZVTQtA8ytkl5iwttxkm6yGIn7r22RTCUP5SfMSuJ13BwJbpNMYDbn9DFMOb5yX4pihVGOE2GstvC0ZCkUSw0rqOWcJ7b0nZJvJMEv2QOGbqlIGxDQ38XZUMlgXdV7iykfHFhUfj6lZaZeSuUAtPAuXrYameYCR0PxFRUus8fQRzR54ccFVM8Qlp1QIJorbWhAn4gcrpv2yZ4iHw3w7T5sX04fgqHS3QhXLknJ1QQG7KiPnZVK4dyJsYdVKu7uHHjZiqFbvHNnJjnYhTWxiw8p0REOD2Ys7zjRoXEpUsec6yI3ycCe079BkoolxsCK9LX3YDnAN93mskK1KAzwoSk5rj5DjZMo7yWCUgDzKojRaSxghmp3a3d30vovKY1jwVgRHuiQPMSycrX9gcFCXVBcyepivQmfgb0cy1oOEBQ1H2cjUW1azunkXXhi5LRe6N4m3yb0NDdXf4ETofPKzqlkioma9rODgHDhooGVeHAVeX8ZcbI7wmsLt5tXRlzaSnOoMOOeV60a1kfopoH9FoI8U1w9hLtQ3OKSDINbK2Y37v06uvTU3xDrTEOxpdPFKTYcM348jmhkvQ1NuMHbviwxLBwkJ8uiiQgJLMjaF76vb9ITU4ZWi0PXRX4JUm13FagtY1XXYQwrRBrf1PkgqW0nA42IDkXc9b68tz85TWhu2pCGv8GXY0pl2HY3pELaCnVuoVSrXf1CRd01eR3MvUBxjGkf96Ne1SNJc0TlHgifJdpOXm8s5JucMMmRowfSItw5QFh9QUmr1UCGcoEgb9R0Z2IhjzlSxfQi3iGqDkpUuqyJNADDLUV1gPLPNZs7Hwr9qIuHsDrM3o4cGv02hxsUJrD4UJDU61sZH1nYSgw23hNRcMWk1pvGsIP2FvorHuVTtqeOsQfiuvyXK1cGdaVdg0yYiLBscMLeqtSPR2Ron5fWDixipcoQnv0SNywAXASVYBmyrzcwkIw6lDrCIbO1i7sl9G72xgCqh4LZBLdpMzTV4iDKbKfTtEvXw7jxerwYFMrjuqRYPCDD9ib2ilabMHBYZpDKeboPvayTtVHkSlMvMjZ6TmIg1xDd90FrcrJhdaxtj1BlNGMklHVV67rnHwaWM6pTYuonmpngnC3pQROHN3WgVmAmoHoo2cKjkydQhJYvXXb8n0G7yIMYGK6kYxuywDzLtW6m3QrpSVU5IkYGKXREMXoMVyCgLPnlGS1SDOoZ7o4Sd4aneGzIpwgf0V6CRyzHlFHs53U', ST_GeomFromText('POINT(-48.613822 58.177156)'), 'sample_NIqn4EcfgmkPWdqf11UhUiQ1CAPUpVS20HaEMDm1yzjsXfcztTawxeClMO6jo0F1SEPriciKLR7hP7', X'EA9BBB44C8BB4C', 'z,y', NULL);
INSERT IGNORE INTO subset3_ref_ebeb1651_t3 (c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12, c13, c14, c15) VALUES (8, 11, 3, 2001, '2023-01-01 22:13:38', 72, 29, 1, 64, 'sample_oG0iTY6hPBvtlvvdm5OXretz9ikkpW4HvUV1KcB7UmuQUJvLefGInw5tw6WUVHvRT0QfIKM4XukK3Ax4cImizTWc0AGdy6wR9DfrgSQfWLjaECuvm3HaUMChCtOatguQiK5q9yGSQylvvcS7f5SUP7TvPDkrbQwEgYKDRjMvIqT3Dx6RbR53QPpJKkOMzDbqSwkj54S12NRc26SSGCVMRb6HzIRTgevYtyZ4DXGGWubmd5oZkKrqjJChEsiWM7wJtLJht3EDUqCfoEkZjXrd1eGRob5u8X2XTX9w0o5ddoP3P8H21ZGKVpeggrOftw8PoDax6KHRuU1kizjwJ4YuOKn7uVuZtmL2hcFfUFdi8mCw5wNTqKydH2THMc3q1rMwAeUBuSKL3sl3SvwLafKqrstQZtZSoJWgFXc2L8pGbYZ9gg6xgAIt7xNyudPhenYqcr9trbA952GZL9fqx5t0HRGqeXG0xXUKHj24TLxtSCo94jRpsow38uscYn4kOGpkooFOQ5IVjRiIS9FYRV9Qf9cPX9KxC4QMo003FjRZaLCgTXn83zgH6dGVSpWYSaqvhJtvtRmrwAMRCw1cccn52MVCvNHkTWg5Jj1gALuJrxysuNqseEK8YWNLSGIBZvrgBcmu1s0rEgq9t3YKRucOBAKFRAQxmulAO3FVnco16YL4tKueN78HHqaytACCC6yjU1C1NpAUjvVRmDoE9Ww4dCp7WwTahdlpvbFljY4puZ9EpbHk5o0JOJTBl6wVXWc1BADCN7GL1iS3UNbEPQop1Joihypbzfb5EYtrUqD3d7VskDDhdLZm8kM4lPxnerIy7FEPtvQ7kagHV145oQxNYMRX8QoyRl5FM9PPTbouEu4LEMqv3PBZDx9tJ29gwFK8IAOJ0zgs6u8g0SdouG6oTHnREutSdGjKcthv3OBK4XgFDjwcg1UqSR10lkdXzDbxKPwAsV147HMlijjryakKLJREgPjR8yT9hIoam6vz8Js5fNZow92iXxGXg8Vxf08RbiUReMRDMlFxrtILyg0lI8icVReeyG5yJWmOEQEf4OZuK1rFJqRVyojsqXl075qrGbmLBkcQjvNnFguVDaGeV0jOwYFSYrjuLKgbRP5sAGCUVE0PBb5dMw8Tec0CGFxq2QPuL0sZOqOUpi7yg9ZkMoXcU7JV5NVP3WQnTufBOBAzX3cH1bNWHJ5XF4tIotjEo31yrlQVGJeidAoIHQBCyA6wAJAxyQjkVltCNgJDbcGVSapMM8oTmLuqwifxbpylZK0Dvc3YnG8iO8cmLCrzwAsvIfvmBVO5VORkIm6ZaLzLNxB2l8IGFEX3eR5Lyxc00wF7qgncghBi0IXt32vBtoaq68VWFKzJ4pYurIX9MBt9uAyJHMmEDgGkU2d45R5Yov9a27JQg3diiT3B4BDm3hIXI1m3mH76wQRxHKrHi0sZAQ89oFgtpRMsXj56YXuzxzOgabNa2jk5mYmxHTgXApLPHmx4o2Rc6NsnvVBnPwoDcDhywDZoGTzaXtXJmK6C0hLCz5XDZ0m2dfGVsUNiWH3YjSnnAqWlxCjJNjjXHbaJwW2H24kFrUMcNK2UAEwtLsg7KzBRFWJ8QV4uJRV20mGHMAQBTPjcQWNwAYjeDthNQPG9IAjWvTcAKMlZ50fczjNT72SQGZGticd3HB1U17ydwPc2tLsH6B4A5yunih4UZNrLj2OYDCkiEiQTSSi93uq9Qy58CqyTsSgsUnq4RBbfCfggm4V', ST_GeomFromText('POINT(-4.209608 -28.055321)'), 'sample_sZWWK7eZagpeO4', X'EC8184E29ABBDFA3DABBE197BFEAB1B4EAA2B0EFA7A64DEABEBAEAA3B73C6C', 'z,y,x', NULL);
INSERT IGNORE INTO subset3_ref_ebeb1651_t3 (c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12, c13, c14, c15) VALUES (9, 16, 6, 2023, '2023-01-01 04:47:57', 63, 66, 4, 51, 'sample_1i5ng25NRkd3uJnegmJDpm3yqDdMlSS5GtEfavIaOYkMlg7LTn7yaDNkaCzjZKPDXZIe5JecebmcbKJCrZv4RZEoV1BvFTMKRvshGCTmI2pKE5jCR9ePalZvkDqb2M9kQr5QPfE3Pt4deaElvfnUMehwBqjQOOHmkTTFuw3aGFo0lPaZ6azuDcdjIi5C2NC14rPnPbiwwZFBogLBU3TmDjzLrPW3pKX94vQvEigaNnAiB7LV7oZfM1dmRBpK3pQCQMyCJrI1uObEE0CaBr6od6NTCcvc2mbj5Yo8x42kH4tc6VUxlvJQzf47YnlM5r1fAOtgd6ItMsw9QUXQQWhu5STg7UUc1MsxV9rToOQkvzpo3NH2LCP2DG75TIMS4mwPxDEvJvoBsnoinobLUUi5Vq35xxSXgSg3EwbWmjKx2wGRD0ukPziXCwyI6IlsYMp3PnEc9TZmaJG6zq1rTcP5J7Y3UXxW9wjzO7Z60SChGvGZNpVpkEymAu9ZK7zjFAh10sEnGy6OpzeVAJRPy6WqrujsFDiufQeQdrW6AWiecdlJhVAHOHtb6aHcJfVkyac7BVTCcNznrjJsWIZFVOaSM78APe57D3Wiz0GHypwFTnxKWVlRk0atFOmnk7AEW87ImFhClyN3dN2aaYkU86gbVufqeYvpGYz0v8riFOXi16xyd4YYH69d2lluf966vUKso8oM70wFqMjmIjwVKd4bSOcd5009h1Jfzb1LFsV84tNPOEXyam4QxgIhLP2NgsPWhCSaFh7d0U1a5BkrnhY4Zzq3DVuUB00ZPB9FBmfTReu8oPN4ChJkx3GsQff9Krdyla9qLGaPhgoFeseGAGsB2Q1YGmMkSZsdLMKo1ipDFVtcDBS4QnQeP0JaDeyU7xTEF67CmNVFoCBwBr6WPXgX3i0lEsLp023jvUVA40G3Lmg10gxt9Tgrp4FLQcuKbWFgl7ST0nbMkfuJK7P24S4bGkmjlR1oa3xv6YhGPzV7X1D4myZ76Lwn3wbwPuxKdM1bvj9Ihx2wSM0OCgJgavv6reb4ZqsS1AXXB31EyJe3t4Ny6Lx1gHbsgnk86za', ST_GeomFromText('POINT(86.258227 -59.772955)'), 'sample_gHdmKFy7qhxQPrbHw4zjpeXuO5wJwkuRO0QUmDRAV6y7tvbBo1eONAa59GLWF4L6qdK9JuMAVSR9AKPqiyh6ONX5iFM7hPeUf7vR7UW5cktMzGscbj6aI6p5dKetpFSOzO6sO7R6zO6AATuOr65WRs2FyB8Yd8j1sOprl8HAU5Zbx5q', X'5EC584E3809ACCBFECAC8C', 'x,y,z', NULL);
INSERT IGNORE INTO subset3_ref_ebeb1651_t3 (c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12, c13, c14, c15) VALUES (10, 3, 9, 2005, '2023-01-01 22:49:05', 100, 44, 84, 80, 'sample_Gbssk7tn8ITpbYb5AwQuBOaSQEGpFjgphTVlWgWs6lkhfcmzhQHdBU4NN1bVPyiokEPdmmRFBkyuI1AdVSdZTTQDP3asinY6G2JIF6rV9o4q0ysQp2JsiOJ39k7K5lYBG1ck9qabZxdZWv8aVYhqEkDX2f9r2t4YTrAfTh1ecUZhDuy4aWL4Er5bjiEkXp93Iyxm50MzFWM0A15iBLQEkLnJUP3Fq1jTmYcQRT5fH1EZiTkxmHKNgdSlufZpWeluBo4uiSS1CbME3rve3tLA5QsqQSd5tsPAxM3OpUc9wvMN3sfnStDuWbRVetEIy97ep8EsHCIc5PaGxgLzbtWK7yognXdCm7ltpLLjrL2bZI1Gfq9IKud3gNiJdamEPHRWa55SkJwcY7y8WNPymxbGzxuQ9pgKIB23vDxpxpcHtEQSMjGdMEPRxfCaoEdTsDnzJHXjJMDZMM0BeyGxr4YAtyfTwnYmTDcYZBItFztkWNLcdx3hiNnR44LzvPDua8vyfWBvDidBxYCoe8cg65Zbw8Mp2VGcdVL9cS7zVpp6yYqy8TqPMgKosSAm6G6zRbX0JzA1q7qbkbW7Y2fJuuWglDnbqg8w4Osvi8Zv0t5XxmdvGC7yXmJi7A6cVMi7LemDTPtvAqU8kpS14uANv9KuGKEvrejusybWyabIjMQgGS6BpLSmCwAa5Jx5GYFqQGumUCvGh', ST_GeomFromText('POINT(88.652213 155.445372)'), 'sample_KsW1tGFABRl0wA6O9w9jyN0h1lqll94z66kgQKKsiJelZEtEKBRTPqJe5MnX736e6TteBFOMNWZRdSC61Y6otdkeajbRMN', X'E4908AD499C3A0DD962DE9B096DDA4EDB697C989C1BA786913D9BAC999CF86D9B0E0B0A9E38BBFDE81', 'y,z,x', NULL);
INSERT IGNORE INTO subset3_ref_ebeb1651_t3 (`c1`, `c2`, `c3`, `c4`, `c5`, `c6`, `c7`, `c8`, `c9`, `c10`, `c11`, `c12`, `c13`, `c14`, `c15`) VALUES (9000000, 20230101, -1, NULL, '18:40:13', 20230101, -1, 1, 1, 'nfyuvwel', NULL, 'lrjx46tbdlkc', NULL, 'x,y,z', -1);
SELECT (COALESCE(`sub1`.`c4`, 0) + COALESCE(`sub1`.`c6`, 0)), CONCAT(COALESCE(`sub1`.`c2`, ''), COALESCE(`sub1`.`c4`, '')), (CASE WHEN `sub1`.`c3` IS NULL THEN 'missing' ELSE `sub1`.`c3` END), UPPER(`sub1`.`c6`) FROM `subset3_ebeb1651` sub1 WHERE (`sub1`.`c5` IS NOT NULL OR `sub1`.`c6` >= '2023-01-01') AND `sub1`.`c4` = -6 AND EXISTS (SELECT 1 FROM `subset3_ref_ebeb1651_t3` ex2 WHERE `ex2`.`c4` = `ex2`.`c10` AND `sub1`.`c2` = `ex2`.`c4`) ORDER BY `sub1`.`c6` DESC;
EXPLAIN FORMAT=TRADITIONAL SELECT (COALESCE(`sub1`.`c4`, 0) + COALESCE(`sub1`.`c6`, 0)), CONCAT(COALESCE(`sub1`.`c2`, ''), COALESCE(`sub1`.`c4`, '')), (CASE WHEN `sub1`.`c3` IS NULL THEN 'missing' ELSE `sub1`.`c3` END), UPPER(`sub1`.`c6`) FROM `subset3_ebeb1651` sub1 WHERE (`sub1`.`c5` IS NOT NULL OR `sub1`.`c6` >= '2023-01-01') AND `sub1`.`c4` = -6 AND EXISTS (SELECT 1 FROM `subset3_ref_ebeb1651_t3` ex2 WHERE `ex2`.`c4` = `ex2`.`c10` AND `sub1`.`c2` = `ex2`.`c4`) ORDER BY `sub1`.`c6` DESC;
START TRANSACTION;
INSERT IGNORE INTO subset3_ebeb1651 (`c1`, `c2`, `c3`, `c4`, `c5`, `c6`) VALUES (6756678, 'hv_7711', 'hv_4652', -6, '2023-01-01', '2023-01-01');
INSERT IGNORE INTO subset3_ebeb1651 (`c1`, `c2`, `c3`, `c4`, `c5`, `c6`) VALUES (9980420, 'hv_7711', 'hv_4652', 16, 'not-a-date', 'not-a-date');
INSERT IGNORE INTO subset3_ebeb1651 (`c1`, `c2`, `c3`, `c4`, `c5`, `c6`) VALUES (8610983, '2023-01-01', 'hv_4652', -4, '0000-00-00', '01e0');
INSERT IGNORE INTO subset3_ebeb1651 (`c1`, `c2`, `c3`, `c4`, `c5`, `c6`) VALUES (7412868, 'hv_7711', 'hv_4652', 16, '0000-00-00', 'hv_4224');
INSERT IGNORE INTO subset3_ebeb1651 (`c1`, `c2`, `c3`, `c4`, `c5`, `c6`) VALUES (6074125, 'hv_7711', 'not-a-date', 16, '2023-01-01', '2023-01-01');
INSERT IGNORE INTO subset3_ebeb1651 (`c1`, `c2`, `c3`, `c4`, `c5`, `c6`) VALUES (8228623, '2023-01-01', 'hv_4652', -597, 'not-a-date', '2023-01-01');
INSERT IGNORE INTO subset3_ebeb1651 (`c1`, `c2`, `c3`, `c4`, `c5`, `c6`) VALUES (4251096, 'not-a-date', '2023-01-01', -10, 'not-a-date', 'hv_4224');
INSERT IGNORE INTO subset3_ebeb1651 (`c1`, `c2`, `c3`, `c4`, `c5`, `c6`) VALUES (184056, 'hv_7711', '2023-01-01', -4, 'not-a-date', 'hv_4224');
INSERT IGNORE INTO subset3_ebeb1651 (`c1`, `c2`, `c3`, `c4`, `c5`, `c6`) VALUES (8616250, 'not-a-date', 'hv_4652', 853, 'not-a-date', 'not-a-date');
INSERT IGNORE INTO subset3_ebeb1651 (`c1`, `c2`, `c3`, `c4`, `c5`, `c6`) VALUES (5590906, 'not-a-date', '2023-01-01', 16, '2023-01-01', 'hv_4224');
INSERT IGNORE INTO subset3_ebeb1651 (`c1`, `c2`, `c3`, `c4`, `c5`, `c6`) VALUES (8273828, 'hv_7711', 'not-a-date', -597, '0000-00-00', 'not-a-date');
INSERT IGNORE INTO subset3_ebeb1651 (`c1`, `c2`, `c3`, `c4`, `c5`, `c6`) VALUES (6960974, '2023-01-01', 'hv_4652', -10, '', 'hv_4224');
INSERT IGNORE INTO subset3_ebeb1651 (`c1`, `c2`, `c3`, `c4`, `c5`, `c6`) VALUES (3583413, 'not-a-date', '2023-01-01', 16, '0000-00-00', '2023-01-01');
INSERT IGNORE INTO subset3_ebeb1651 (`c1`, `c2`, `c3`, `c4`, `c5`, `c6`) VALUES (5259829, 'hv_7711', '2023-01-01', 509, 'not-a-date', 'd4:-stj3rc');
INSERT IGNORE INTO subset3_ebeb1651 (`c1`, `c2`, `c3`, `c4`, `c5`, `c6`) VALUES (6071768, 'hv_7711', '2023-01-01', -10, 'not-a-date', 'hv_4224');
INSERT IGNORE INTO subset3_ebeb1651 (`c1`, `c2`, `c3`, `c4`, `c5`, `c6`) VALUES (911833, 'not-a-date', '2023-01-01', 16, '0000-00-00', 'hv_4224');
INSERT IGNORE INTO subset3_ebeb1651 (`c1`, `c2`, `c3`, `c4`, `c5`, `c6`) VALUES (2727311, 'not-a-date', 'not-a-date', -10, 'not-a-date', '2023-01-01');
INSERT IGNORE INTO subset3_ebeb1651 (`c1`, `c2`, `c3`, `c4`, `c5`, `c6`) VALUES (1891344, 'not-a-date', 'hv_4652', -6, '0000-00-00', '2023-01-01');
INSERT IGNORE INTO subset3_ebeb1651 (`c1`, `c2`, `c3`, `c4`, `c5`, `c6`) VALUES (9056620, 'not-a-date', '2023-01-01', -4, 'not-a-date', 'hv_4224');
INSERT IGNORE INTO subset3_ebeb1651 (`c1`, `c2`, `c3`, `c4`, `c5`, `c6`) VALUES (6502598, ' 1', 'not-a-date', -10, 'not-a-date', '');
INSERT IGNORE INTO subset3_ebeb1651 (`c1`, `c2`, `c3`, `c4`, `c5`, `c6`) VALUES (5428590, '01e0', 'hv_4652', -10, '2023-01-01', '2023-01-01');
INSERT IGNORE INTO subset3_ebeb1651 (`c1`, `c2`, `c3`, `c4`, `c5`, `c6`) VALUES (5291849, 'hv_7711', 'hv_4652', -10, 'not-a-date', 'hv_4224');
INSERT IGNORE INTO subset3_ebeb1651 (`c1`, `c2`, `c3`, `c4`, `c5`, `c6`) VALUES (9901639, 'hv_7711', '2023-01-01', 16, '2023-01-01', 'not-a-date');
INSERT IGNORE INTO subset3_ebeb1651 (`c1`, `c2`, `c3`, `c4`, `c5`, `c6`) VALUES (3720247, 'not-a-date', 'not-a-date', -6, '2023-01-01', 'hv_4224');
INSERT IGNORE INTO subset3_ebeb1651 (`c1`, `c2`, `c3`, `c4`, `c5`, `c6`) VALUES (3787713, '2023-01-01', 'not-a-date', -734, '2023-01-01', '2023-01-01');
INSERT IGNORE INTO subset3_ebeb1651 (`c1`, `c2`, `c3`, `c4`, `c5`, `c6`) VALUES (175337, 'vxhpd_1qcpc', '2023-01-01', -4, '2023-01-01', '2023-01-01');
INSERT IGNORE INTO subset3_ebeb1651 (`c1`, `c2`, `c3`, `c4`, `c5`, `c6`) VALUES (7509563, 'not-a-date', '2023-01-01', -10, 'not-a-date', 'hv_4224');
INSERT IGNORE INTO subset3_ebeb1651 (`c1`, `c2`, `c3`, `c4`, `c5`, `c6`) VALUES (6333095, '2023-01-01', '8:c49g xo', -10, '0000-00-00', '2023-01-01');
INSERT IGNORE INTO subset3_ebeb1651 (`c1`, `c2`, `c3`, `c4`, `c5`, `c6`) VALUES (5534134, '2023-01-01', '2023-01-01', -4, '0000-00-00', 'not-a-date');
INSERT IGNORE INTO subset3_ebeb1651 (`c1`, `c2`, `c3`, `c4`, `c5`, `c6`) VALUES (3097222, 'hv_7711', 'hv_4652', -4, 'not-a-date', '2023-01-01');
INSERT IGNORE INTO subset3_ebeb1651 (`c1`, `c2`, `c3`, `c4`, `c5`, `c6`) VALUES (5874184, 'h3mr4em_ju  lu_n-s', '2023-01-01', -10, '0000-00-00', 'hv_4224');
INSERT IGNORE INTO subset3_ebeb1651 (`c1`, `c2`, `c3`, `c4`, `c5`, `c6`) VALUES (4778172, '2023-01-01', '', -10, '2023-01-01', '0-3/3eri0ul:ld :ywg:');
INSERT IGNORE INTO subset3_ebeb1651 (`c1`, `c2`, `c3`, `c4`, `c5`, `c6`) VALUES (6266774, 'hv_7711', '0', 16, '2023-01-01', '2023-01-01');
INSERT IGNORE INTO subset3_ebeb1651 (`c1`, `c2`, `c3`, `c4`, `c5`, `c6`) VALUES (6837689, 'not-a-date', 'not-a-date', -4, '2023-01-01', 'not-a-date');
INSERT IGNORE INTO subset3_ebeb1651 (`c1`, `c2`, `c3`, `c4`, `c5`, `c6`) VALUES (5470610, '2023-01-01', 'hv_4652', -552, '0000-00-00', 'not-a-date');
INSERT IGNORE INTO subset3_ebeb1651 (`c1`, `c2`, `c3`, `c4`, `c5`, `c6`) VALUES (2687036, 'not-a-date', 'not-a-date', -10, '2023-01-01', 'not-a-date');
INSERT IGNORE INTO subset3_ebeb1651 (`c1`, `c2`, `c3`, `c4`, `c5`, `c6`) VALUES (9183989, 'not-a-date', 'not-a-date', -4, '2023-01-01', '2023-01-01');
INSERT IGNORE INTO subset3_ebeb1651 (`c1`, `c2`, `c3`, `c4`, `c5`, `c6`) VALUES (8731071, 'hv_7711', 'not-a-date', -10, '0000-00-00', 'hv_4224');
INSERT IGNORE INTO subset3_ebeb1651 (`c1`, `c2`, `c3`, `c4`, `c5`, `c6`) VALUES (1085335, 'not-a-date', 'hv_4652', 16, 'not-a-date', '2023-01-01');
INSERT IGNORE INTO subset3_ebeb1651 (`c1`, `c2`, `c3`, `c4`, `c5`, `c6`) VALUES (4738837, 'hv_7711', '-1', -6, 'not-a-date', 'hv_4224');
INSERT IGNORE INTO subset3_ebeb1651 (`c1`, `c2`, `c3`, `c4`, `c5`, `c6`) VALUES (9019406, '2023-01-01', 'not-a-date', 16, '0000-00-00', 'hv_4224');
INSERT IGNORE INTO subset3_ebeb1651 (`c1`, `c2`, `c3`, `c4`, `c5`, `c6`) VALUES (3053111, '2023-01-01', 'ibg8zhka:lm/li', -10, '0000-00-00', '2023-01-01');
INSERT IGNORE INTO subset3_ebeb1651 (`c1`, `c2`, `c3`, `c4`, `c5`, `c6`) VALUES (9361444, 'not-a-date', '2023-01-01', 16, '0000-00-00', 'not-a-date');
INSERT IGNORE INTO subset3_ebeb1651 (`c1`, `c2`, `c3`, `c4`, `c5`, `c6`) VALUES (6928283, '2023-01-01', 'hv_4652', 733, 'not-a-date', '2023-01-01');
INSERT IGNORE INTO subset3_ebeb1651 (`c1`, `c2`, `c3`, `c4`, `c5`, `c6`) VALUES (8019269, 'not-a-date', 'not-a-date', -10, '2023-01-01', '2023-01-01');
INSERT IGNORE INTO subset3_ebeb1651 (`c1`, `c2`, `c3`, `c4`, `c5`, `c6`) VALUES (9370667, 'not-a-date', 'not-a-date', -4, '0000-00-00', '2023-01-01');
INSERT IGNORE INTO subset3_ebeb1651 (`c1`, `c2`, `c3`, `c4`, `c5`, `c6`) VALUES (9125079, 'not-a-date', 'not-a-date', 412, '0000-00-00', 'hv_4224');
INSERT IGNORE INTO subset3_ebeb1651 (`c1`, `c2`, `c3`, `c4`, `c5`, `c6`) VALUES (41247, '073a:fu3b61', 'not-a-date', -6, '0000-00-00', '2023-01-01');
INSERT IGNORE INTO subset3_ebeb1651 (`c1`, `c2`, `c3`, `c4`, `c5`, `c6`) VALUES (6806223, '2023-01-01', '2023-01-01', 16, 'not-a-date', 'o1mgb183-oyd/_bjn1');
INSERT IGNORE INTO subset3_ebeb1651 (`c1`, `c2`, `c3`, `c4`, `c5`, `c6`) VALUES (4051166, 'not-a-date', '2023-01-01', -4, 'not-a-date', 'not-a-date');
INSERT IGNORE INTO subset3_ebeb1651 (`c1`, `c2`, `c3`, `c4`, `c5`, `c6`) VALUES (6935057, 'hv_7711', '2023-01-01', 16, '2023-01-01', '0000-00-00');
INSERT IGNORE INTO subset3_ebeb1651 (`c1`, `c2`, `c3`, `c4`, `c5`, `c6`) VALUES (7384411, 'not-a-date', 'hv_4652', -10, '2023-01-01', 'not-a-date');
INSERT IGNORE INTO subset3_ebeb1651 (`c1`, `c2`, `c3`, `c4`, `c5`, `c6`) VALUES (7408820, '2023-01-01', 'not-a-date', 16, '2023-01-01', 'not-a-date');
INSERT IGNORE INTO subset3_ebeb1651 (`c1`, `c2`, `c3`, `c4`, `c5`, `c6`) VALUES (4252809, 'hv_7711', '2023-01-01', -4, '2023-01-01', 'hv_4224');
INSERT IGNORE INTO subset3_ebeb1651 (`c1`, `c2`, `c3`, `c4`, `c5`, `c6`) VALUES (6590201, '2023-01-01', 'hv_4652', 16, '0000-00-00', 'hv_4224');
INSERT IGNORE INTO subset3_ebeb1651 (`c1`, `c2`, `c3`, `c4`, `c5`, `c6`) VALUES (7754936, '2023-01-01', 'not-a-date', 16, '0000-00-00', '2023-01-01');
INSERT IGNORE INTO subset3_ebeb1651 (`c1`, `c2`, `c3`, `c4`, `c5`, `c6`) VALUES (2187693, 'not-a-date', 'hv_4652', 16, '2023-01-01', 'hv_4224');
INSERT IGNORE INTO subset3_ebeb1651 (`c1`, `c2`, `c3`, `c4`, `c5`, `c6`) VALUES (6723947, 'hv_7711', 'hv_4652', -6, 'not-a-date', '2023-01-01');
INSERT IGNORE INTO subset3_ebeb1651 (`c1`, `c2`, `c3`, `c4`, `c5`, `c6`) VALUES (2193472, 'hv_7711', 'not-a-date', -6, '2023-01-01', 'hv_4224');
INSERT IGNORE INTO subset3_ebeb1651 (`c1`, `c2`, `c3`, `c4`, `c5`, `c6`) VALUES (1628142, '2023-01-01', 'hv_4652', 16, 'not-a-date', 'not-a-date');
INSERT IGNORE INTO subset3_ebeb1651 (`c1`, `c2`, `c3`, `c4`, `c5`, `c6`) VALUES (6279299, 'not-a-date', 'not-a-date', 16, 'not-a-date', 'not-a-date');
INSERT IGNORE INTO subset3_ebeb1651 (`c1`, `c2`, `c3`, `c4`, `c5`, `c6`) VALUES (1830670, '2023-01-01', '2023-01-01', -10, 'not-a-date', 'hv_4224');
INSERT IGNORE INTO subset3_ebeb1651 (`c1`, `c2`, `c3`, `c4`, `c5`, `c6`) VALUES (1384376, 'hv_7711', 'hv_4652', -47, 'not-a-date', 'not-a-date');
INSERT IGNORE INTO subset3_ebeb1651 (`c1`, `c2`, `c3`, `c4`, `c5`, `c6`) VALUES (1986548, 'not-a-date', ' 1', -6, 'not-a-date', '2023-01-01');
INSERT IGNORE INTO subset3_ebeb1651 (`c1`, `c2`, `c3`, `c4`, `c5`, `c6`) VALUES (5509358, 'not-a-date', 'not-a-date', -287, 'not-a-date', ' 1');
INSERT IGNORE INTO subset3_ebeb1651 (`c1`, `c2`, `c3`, `c4`, `c5`, `c6`) VALUES (6807749, 'hv_7711', '2023-01-01', 16, '2023-01-01', 'not-a-date');
INSERT IGNORE INTO subset3_ebeb1651 (`c1`, `c2`, `c3`, `c4`, `c5`, `c6`) VALUES (5722371, '2023-01-01', '2023-01-01', -575, '0000-00-00', '2023-01-01');
INSERT IGNORE INTO subset3_ebeb1651 (`c1`, `c2`, `c3`, `c4`, `c5`, `c6`) VALUES (2038020, 'zord1btm', 'r04cgz3z/er1/c7/l', 16, '2023-01-01', 'not-a-date');
INSERT IGNORE INTO subset3_ebeb1651 (`c1`, `c2`, `c3`, `c4`, `c5`, `c6`) VALUES (5502573, 'hv_7711', 'not-a-date', 16, '2023-01-01', 'not-a-date');
INSERT IGNORE INTO subset3_ebeb1651 (`c1`, `c2`, `c3`, `c4`, `c5`, `c6`) VALUES (8949657, '2023-01-01', 'not-a-date', 16, 'not-a-date', 'hv_4224');
INSERT IGNORE INTO subset3_ebeb1651 (`c1`, `c2`, `c3`, `c4`, `c5`, `c6`) VALUES (4771393, 'not-a-date', '2023-01-01', -10, '2023-01-01', '2023-01-01');
INSERT IGNORE INTO subset3_ebeb1651 (`c1`, `c2`, `c3`, `c4`, `c5`, `c6`) VALUES (7894319, '2023-01-01', '2023-01-01', 823, '1996-10-16', '2023-01-01');
INSERT IGNORE INTO subset3_ebeb1651 (`c1`, `c2`, `c3`, `c4`, `c5`, `c6`) VALUES (2154055, 'not-a-date', 'not-a-date', 16, '0000-00-00', 'hv_4224');
INSERT IGNORE INTO subset3_ebeb1651 (`c1`, `c2`, `c3`, `c4`, `c5`, `c6`) VALUES (7119712, 'not-a-date', '1', -10, 'not-a-date', 'not-a-date');
INSERT IGNORE INTO subset3_ebeb1651 (`c1`, `c2`, `c3`, `c4`, `c5`, `c6`) VALUES (225790, 'hv_7711', '2023-01-01', -4, 'not-a-date', 'not-a-date');
INSERT IGNORE INTO subset3_ebeb1651 (`c1`, `c2`, `c3`, `c4`, `c5`, `c6`) VALUES (4577321, 'not-a-date', 'ded9fm2f', -4, '2023-01-01', '2023-01-01');
INSERT IGNORE INTO subset3_ebeb1651 (`c1`, `c2`, `c3`, `c4`, `c5`, `c6`) VALUES (4683755, '2023-01-01', '2023-01-01', 16, 'not-a-date', 'hv_4224');
INSERT IGNORE INTO subset3_ebeb1651 (`c1`, `c2`, `c3`, `c4`, `c5`, `c6`) VALUES (8437117, 'hv_7711', 'hv_4652', -6, '2011-05-04', '2023-01-01');
INSERT IGNORE INTO subset3_ebeb1651 (`c1`, `c2`, `c3`, `c4`, `c5`, `c6`) VALUES (7173546, 'not-a-date', 'hv_4652', -10, '0000-00-00', '2023-01-01');
INSERT IGNORE INTO subset3_ebeb1651 (`c1`, `c2`, `c3`, `c4`, `c5`, `c6`) VALUES (2717711, 'not-a-date', 'not-a-date', 16, '2026-11-16', 'hv_4224');
INSERT IGNORE INTO subset3_ebeb1651 (`c1`, `c2`, `c3`, `c4`, `c5`, `c6`) VALUES (4991920, 'not-a-date', 'hv_4652', 522, '2034-02-12', 'hv_4224');
INSERT IGNORE INTO subset3_ebeb1651 (`c1`, `c2`, `c3`, `c4`, `c5`, `c6`) VALUES (1129950, '2023-01-01', '2023-01-01', -6, '', 'not-a-date');
INSERT IGNORE INTO subset3_ebeb1651 (`c1`, `c2`, `c3`, `c4`, `c5`, `c6`) VALUES (6336921, '2023-01-01', '2023-01-01', -10, 'not-a-date', 'not-a-date');
INSERT IGNORE INTO subset3_ebeb1651 (`c1`, `c2`, `c3`, `c4`, `c5`, `c6`) VALUES (8646407, 'not-a-date', 'not-a-date', -10, '2023-01-01', 'hv_4224');
INSERT IGNORE INTO subset3_ebeb1651 (`c1`, `c2`, `c3`, `c4`, `c5`, `c6`) VALUES (2783284, 'hv_7711', 'hv_4652', 16, 'not-a-date', 'not-a-date');
INSERT IGNORE INTO subset3_ebeb1651 (`c1`, `c2`, `c3`, `c4`, `c5`, `c6`) VALUES (2640747, 'hv_7711', '804rntsbtweq7', -10, '0000-00-00', 'hv_4224');
INSERT IGNORE INTO subset3_ebeb1651 (`c1`, `c2`, `c3`, `c4`, `c5`, `c6`) VALUES (6199868, 'hv_7711', 'not-a-date', -6, 'not-a-date', 'not-a-date');
INSERT IGNORE INTO subset3_ebeb1651 (`c1`, `c2`, `c3`, `c4`, `c5`, `c6`) VALUES (4288021, 'hv_7711', 'not-a-date', -29, 'not-a-date', 'hv_4224');
INSERT IGNORE INTO subset3_ebeb1651 (`c1`, `c2`, `c3`, `c4`, `c5`, `c6`) VALUES (3007807, 'not-a-date', 'not-a-date', -6, '0000-00-00', '2023-01-01');
INSERT IGNORE INTO subset3_ebeb1651 (`c1`, `c2`, `c3`, `c4`, `c5`, `c6`) VALUES (7423090, 'not-a-date', 'hv_4652', 16, 'not-a-date', '2023-01-01');
INSERT IGNORE INTO subset3_ebeb1651 (`c1`, `c2`, `c3`, `c4`, `c5`, `c6`) VALUES (7028155, '2023-01-01', '2023-01-01', 16, '2023-01-01', 'not-a-date');
INSERT IGNORE INTO subset3_ebeb1651 (`c1`, `c2`, `c3`, `c4`, `c5`, `c6`) VALUES (833569, 'hv_7711', 'not-a-date', -6, '2023-01-01', '2023-01-01');
INSERT IGNORE INTO subset3_ebeb1651 (`c1`, `c2`, `c3`, `c4`, `c5`, `c6`) VALUES (5275312, 'not-a-date', '2023-01-01', 435, '0000-00-00', '2023-01-01');
INSERT IGNORE INTO subset3_ebeb1651 (`c1`, `c2`, `c3`, `c4`, `c5`, `c6`) VALUES (1804850, '7p-ycff7 1ctmwpzj9ox', 'not-a-date', -6, '0000-00-00', 'not-a-date');
INSERT IGNORE INTO subset3_ebeb1651 (`c1`, `c2`, `c3`, `c4`, `c5`, `c6`) VALUES (86585, '2023-01-01', 'hv_4652', 16, 'not-a-date', 'hv_4224');
INSERT IGNORE INTO subset3_ebeb1651 (`c1`, `c2`, `c3`, `c4`, `c5`, `c6`) VALUES (7196734, 'hv_7711', 'hv_4652', -4, '0000-00-00', 'not-a-date');
INSERT IGNORE INTO subset3_ebeb1651 (`c1`, `c2`, `c3`, `c4`, `c5`, `c6`) VALUES (1962556, 'hv_7711', 'hv_4652', 16, '0000-00-00', 'hv_4224');
INSERT IGNORE INTO subset3_ebeb1651 (`c1`, `c2`, `c3`, `c4`, `c5`, `c6`) VALUES (7662547, '2023-01-01', '2023-01-01', -4, '2023-01-01', '2023-01-01');
INSERT IGNORE INTO subset3_ebeb1651 (`c1`, `c2`, `c3`, `c4`, `c5`, `c6`) VALUES (8690867, '2023-01-01', 'not-a-date', -4, '0000-00-00', 'hv_4224');
INSERT IGNORE INTO subset3_ebeb1651 (`c1`, `c2`, `c3`, `c4`, `c5`, `c6`) VALUES (937951, 'not-a-date', 'hv_4652', -6, '0000-00-00', 'hv_4224');
INSERT IGNORE INTO subset3_ebeb1651 (`c1`, `c2`, `c3`, `c4`, `c5`, `c6`) VALUES (5810310, ' 1', '', -10, '0000-00-00', 'not-a-date');
INSERT IGNORE INTO subset3_ebeb1651 (`c1`, `c2`, `c3`, `c4`, `c5`, `c6`) VALUES (8361409, 'hv_7711', 'hv_4652', 16, '0000-00-00', 'hv_4224');
INSERT IGNORE INTO subset3_ebeb1651 (`c1`, `c2`, `c3`, `c4`, `c5`, `c6`) VALUES (1300788, '2023-01-01', 'not-a-date', 738, '2023-01-01', 'not-a-date');
INSERT IGNORE INTO subset3_ebeb1651 (`c1`, `c2`, `c3`, `c4`, `c5`, `c6`) VALUES (4104047, '2023-01-01', 'not-a-date', 16, '0000-00-00', 'not-a-date');
INSERT IGNORE INTO subset3_ebeb1651 (`c1`, `c2`, `c3`, `c4`, `c5`, `c6`) VALUES (3590291, 'not-a-date', 'not-a-date', -6, '0000-00-00', '2023-01-01');
INSERT IGNORE INTO subset3_ebeb1651 (`c1`, `c2`, `c3`, `c4`, `c5`, `c6`) VALUES (2753782, 'hv_7711', 'not-a-date', 16, '0000-00-00', 'hv_4224');
INSERT IGNORE INTO subset3_ebeb1651 (`c1`, `c2`, `c3`, `c4`, `c5`, `c6`) VALUES (5884939, 'hv_7711', 'not-a-date', -4, 'not-a-date', 'hv_4224');
INSERT IGNORE INTO subset3_ebeb1651 (`c1`, `c2`, `c3`, `c4`, `c5`, `c6`) VALUES (4385148, 'hv_7711', '2023-01-01', 16, 'not-a-date', 'hv_4224');
INSERT IGNORE INTO subset3_ebeb1651 (`c1`, `c2`, `c3`, `c4`, `c5`, `c6`) VALUES (9984208, '2023-01-01', 'not-a-date', 16, 'not-a-date', 'rbk97l u');
INSERT IGNORE INTO subset3_ebeb1651 (`c1`, `c2`, `c3`, `c4`, `c5`, `c6`) VALUES (9225768, 'not-a-date', '2023-01-01', 16, 'not-a-date', 'hv_4224');
INSERT IGNORE INTO subset3_ebeb1651 (`c1`, `c2`, `c3`, `c4`, `c5`, `c6`) VALUES (6083667, 'not-a-date', 'not-a-date', 16, '2023-01-01', 'not-a-date');
INSERT IGNORE INTO subset3_ebeb1651 (`c1`, `c2`, `c3`, `c4`, `c5`, `c6`) VALUES (2057434, 'x9xb7zw7ymnfc14cofc-', 'not-a-date', 16, '2023-01-01', '2023-01-01');
INSERT IGNORE INTO subset3_ebeb1651 (`c1`, `c2`, `c3`, `c4`, `c5`, `c6`) VALUES (5693157, 'hv_7711', '2023-01-01', 16, '0000-00-00', 'hv_4224');
INSERT IGNORE INTO subset3_ebeb1651 (`c1`, `c2`, `c3`, `c4`, `c5`, `c6`) VALUES (9554549, 'not-a-date', 'hv_4652', -4, '2023-01-01', '2023-01-01');
INSERT IGNORE INTO subset3_ebeb1651 (`c1`, `c2`, `c3`, `c4`, `c5`, `c6`) VALUES (1334337, '2023-01-01', '2023-01-01', 16, '0000-00-00', '2023-01-01');
INSERT IGNORE INTO subset3_ebeb1651 (`c1`, `c2`, `c3`, `c4`, `c5`, `c6`) VALUES (2616338, '2023-01-01', 'not-a-date', -4, 'not-a-date', 'not-a-date');
INSERT IGNORE INTO subset3_ebeb1651 (`c1`, `c2`, `c3`, `c4`, `c5`, `c6`) VALUES (3983040, 'not-a-date', 'hv_4652', 16, 'not-a-date', 'hv_4224');
INSERT IGNORE INTO subset3_ebeb1651 (`c1`, `c2`, `c3`, `c4`, `c5`, `c6`) VALUES (8452426, 'hv_7711', 'not-a-date', 16, 'not-a-date', 'not-a-date');
INSERT IGNORE INTO subset3_ebeb1651 (`c1`, `c2`, `c3`, `c4`, `c5`, `c6`) VALUES (1588095, 'hv_7711', '2023-01-01', 16, '0000-00-00', 'not-a-date');
INSERT IGNORE INTO subset3_ebeb1651 (`c1`, `c2`, `c3`, `c4`, `c5`, `c6`) VALUES (8686842, 'not-a-date', '2023-01-01', 16, 'not-a-date', 'not-a-date');
INSERT IGNORE INTO subset3_ebeb1651 (`c1`, `c2`, `c3`, `c4`, `c5`, `c6`) VALUES (3332562, '2023-01-01', '2023-01-01', -4, 'not-a-date', 'hv_4224');
INSERT IGNORE INTO subset3_ebeb1651 (`c1`, `c2`, `c3`, `c4`, `c5`, `c6`) VALUES (5102615, 'hv_7711', '2023-01-01', -4, 'not-a-date', 'kbu4829n');
INSERT IGNORE INTO subset3_ebeb1651 (`c1`, `c2`, `c3`, `c4`, `c5`, `c6`) VALUES (6078886, '2023-01-01', 'not-a-date', 933, '0000-00-00', 'not-a-date');
INSERT IGNORE INTO subset3_ebeb1651 (`c1`, `c2`, `c3`, `c4`, `c5`, `c6`) VALUES (4560648, '2023-01-01', 'hv_4652', 16, 'not-a-date', '2023-01-01 00:00:00');
INSERT IGNORE INTO subset3_ebeb1651 (`c1`, `c2`, `c3`, `c4`, `c5`, `c6`) VALUES (9675040, 'not-a-date', '2023-01-01', -4, 'not-a-date', 'not-a-date');
INSERT IGNORE INTO subset3_ebeb1651 (`c1`, `c2`, `c3`, `c4`, `c5`, `c6`) VALUES (5554219, '2023-01-01', '2023-01-01', -6, 'not-a-date', 'hv_4224');
INSERT IGNORE INTO subset3_ebeb1651 (`c1`, `c2`, `c3`, `c4`, `c5`, `c6`) VALUES (4004344, 'hv_7711', 'hv_4652', -4, '0000-00-00', '2023-01-01');
INSERT IGNORE INTO subset3_ebeb1651 (`c1`, `c2`, `c3`, `c4`, `c5`, `c6`) VALUES (2785611, '2023-01-01', '2023-01-01', -10, '0000-00-00', 'hv_4224');
INSERT IGNORE INTO subset3_ebeb1651 (`c1`, `c2`, `c3`, `c4`, `c5`, `c6`) VALUES (3071245, 'not-a-date', 'hv_4652', -6, '2023-01-01', '2023-01-01');
INSERT IGNORE INTO subset3_ebeb1651 (`c1`, `c2`, `c3`, `c4`, `c5`, `c6`) VALUES (6935308, 'hv_7711', 'hv_4652', 16, '0000-00-00', 'hv_4224');
INSERT IGNORE INTO subset3_ebeb1651 (`c1`, `c2`, `c3`, `c4`, `c5`, `c6`) VALUES (1520792, '2023-01-01', '2023-01-01', -10, 'not-a-date', 'hv_4224');
INSERT IGNORE INTO subset3_ebeb1651 (`c1`, `c2`, `c3`, `c4`, `c5`, `c6`) VALUES (4295753, '2023-01-01', '2023-01-01', -4, 'not-a-date', 'not-a-date');
INSERT IGNORE INTO subset3_ebeb1651 (`c1`, `c2`, `c3`, `c4`, `c5`, `c6`) VALUES (7481414, 'psvr2f7rf32rdt', 'hv_4652', -6, '2023-01-01', 'hv_4224');
INSERT IGNORE INTO subset3_ebeb1651 (`c1`, `c2`, `c3`, `c4`, `c5`, `c6`) VALUES (9746667, 'hv_7711', 'not-a-date', -4, 'not-a-date', 'not-a-date');
INSERT IGNORE INTO subset3_ebeb1651 (`c1`, `c2`, `c3`, `c4`, `c5`, `c6`) VALUES (3205952, '2023-01-01', 'hv_4652', 16, '2023-01-01', 'hv_4224');
INSERT IGNORE INTO subset3_ebeb1651 (`c1`, `c2`, `c3`, `c4`, `c5`, `c6`) VALUES (4531688, '2023-01-01 00:00:00', 'not-a-date', 16, '0000-00-00', '2023-01-01');
INSERT IGNORE INTO subset3_ebeb1651 (`c1`, `c2`, `c3`, `c4`, `c5`, `c6`) VALUES (8233300, '2023-01-01', 'not-a-date', 16, '2023-01-01', 'not-a-date');
INSERT IGNORE INTO subset3_ebeb1651 (`c1`, `c2`, `c3`, `c4`, `c5`, `c6`) VALUES (7856644, 'not-a-date', 'not-a-date', -153, '0000-00-00', '2023-01-01');
INSERT IGNORE INTO subset3_ebeb1651 (`c1`, `c2`, `c3`, `c4`, `c5`, `c6`) VALUES (9797224, 'hv_7711', 'not-a-date', -10, '0000-00-00', 'hv_4224');
INSERT IGNORE INTO subset3_ebeb1651 (`c1`, `c2`, `c3`, `c4`, `c5`, `c6`) VALUES (6446677, 'hv_7711', 'not-a-date', -6, '2023-01-01', 'not-a-date');
INSERT IGNORE INTO subset3_ebeb1651 (`c1`, `c2`, `c3`, `c4`, `c5`, `c6`) VALUES (6445686, '2023-01-01', '2023-01-01', -10, '0000-00-00', 'hv_4224');
INSERT IGNORE INTO subset3_ebeb1651 (`c1`, `c2`, `c3`, `c4`, `c5`, `c6`) VALUES (4714096, 'hv_7711', 'hv_4652', -4, 'not-a-date', '2023-01-01');
INSERT IGNORE INTO subset3_ebeb1651 (`c1`, `c2`, `c3`, `c4`, `c5`, `c6`) VALUES (684840, 'wjm3ss-jwy:gldpyyta', 'hv_4652', 16, 'not-a-date', '2023-01-01');
INSERT IGNORE INTO subset3_ebeb1651 (`c1`, `c2`, `c3`, `c4`, `c5`, `c6`) VALUES (5783547, '2023-01-01', '2023-01-01', -4, '0000-00-00', '2023-01-01');
INSERT IGNORE INTO subset3_ebeb1651 (`c1`, `c2`, `c3`, `c4`, `c5`, `c6`) VALUES (6191066, 'not-a-date', 'hv_4652', 16, '0000-00-00', '2023-01-01');
INSERT IGNORE INTO subset3_ebeb1651 (`c1`, `c2`, `c3`, `c4`, `c5`, `c6`) VALUES (7325106, '2023-01-01', '2023-01-01', 16, '0000-00-00', 'hv_4224');
INSERT IGNORE INTO subset3_ebeb1651 (`c1`, `c2`, `c3`, `c4`, `c5`, `c6`) VALUES (3141113, '4itbd-gw2', 'hv_4652', -4, '0000-00-00', 'hv_4224');
INSERT IGNORE INTO subset3_ebeb1651 (`c1`, `c2`, `c3`, `c4`, `c5`, `c6`) VALUES (205911, '2023-01-01', 'not-a-date', 16, '0000-00-00', '2023-01-01');
INSERT IGNORE INTO subset3_ebeb1651 (`c1`, `c2`, `c3`, `c4`, `c5`, `c6`) VALUES (6099505, 'not-a-date', 'not-a-date', 16, '2023-01-01', 'hv_4224');
INSERT IGNORE INTO subset3_ebeb1651 (`c1`, `c2`, `c3`, `c4`, `c5`, `c6`) VALUES (232090, 'vd:mq9 40o9ksm-', '2023-01-01', 16, '0000-00-00', ' 1');
INSERT IGNORE INTO subset3_ebeb1651 (`c1`, `c2`, `c3`, `c4`, `c5`, `c6`) VALUES (58119, 'not-a-date', '01e0', 673, 'not-a-date', '2023-01-01');
INSERT IGNORE INTO subset3_ebeb1651 (`c1`, `c2`, `c3`, `c4`, `c5`, `c6`) VALUES (5471577, 'hv_7711', 'hv_4652', -147, '2023-01-01', 'hv_4224');
INSERT IGNORE INTO subset3_ebeb1651 (`c1`, `c2`, `c3`, `c4`, `c5`, `c6`) VALUES (5641365, '0s-zmctl3nki6/', 'hv_4652', 958, '0000-00-00', 'not-a-date');
INSERT IGNORE INTO subset3_ebeb1651 (`c1`, `c2`, `c3`, `c4`, `c5`, `c6`) VALUES (4955258, '2023-01-01', 'not-a-date', -6, 'not-a-date', '2023-01-01');
INSERT IGNORE INTO subset3_ebeb1651 (`c1`, `c2`, `c3`, `c4`, `c5`, `c6`) VALUES (8050203, 'not-a-date', '', -4, 'not-a-date', 'hv_4224');
INSERT IGNORE INTO subset3_ebeb1651 (`c1`, `c2`, `c3`, `c4`, `c5`, `c6`) VALUES (1146379, '2023-01-01', 'hv_4652', -4, '0000-00-00', 'hv_4224');
INSERT IGNORE INTO subset3_ebeb1651 (`c1`, `c2`, `c3`, `c4`, `c5`, `c6`) VALUES (4237932, 'not-a-date', '2023-01-01', 16, '0000-00-00', 'not-a-date');
INSERT IGNORE INTO subset3_ebeb1651 (`c1`, `c2`, `c3`, `c4`, `c5`, `c6`) VALUES (7172966, 'not-a-date', '2023-01-01', 630, '2023-01-01', '2023-01-01');
INSERT IGNORE INTO subset3_ebeb1651 (`c1`, `c2`, `c3`, `c4`, `c5`, `c6`) VALUES (3624570, '2023-01-01', 'not-a-date', -4, 'not-a-date', 'r1wlieb63m/ydip5xr');
INSERT IGNORE INTO subset3_ebeb1651 (`c1`, `c2`, `c3`, `c4`, `c5`, `c6`) VALUES (8050755, 'hv_7711', 'not-a-date', -4, '2023-01-01', 'not-a-date');
INSERT IGNORE INTO subset3_ebeb1651 (`c1`, `c2`, `c3`, `c4`, `c5`, `c6`) VALUES (8008190, 'not-a-date', 'hv_4652', -4, '0000-00-00', 'not-a-date');
INSERT IGNORE INTO subset3_ebeb1651 (`c1`, `c2`, `c3`, `c4`, `c5`, `c6`) VALUES (8256281, 'not-a-date', '2023-01-01', -6, 'not-a-date', 'hv_4224');
INSERT IGNORE INTO subset3_ebeb1651 (`c1`, `c2`, `c3`, `c4`, `c5`, `c6`) VALUES (8150301, 'hv_7711', '2023-01-01', -6, '0000-00-00', '');
INSERT IGNORE INTO subset3_ebeb1651 (`c1`, `c2`, `c3`, `c4`, `c5`, `c6`) VALUES (5149742, 'not-a-date', 'not-a-date', -4, 'not-a-date', 'not-a-date');
INSERT IGNORE INTO subset3_ebeb1651 (`c1`, `c2`, `c3`, `c4`, `c5`, `c6`) VALUES (5742362, '2023-01-01', 'hv_4652', 16, '2023-01-01', 'not-a-date');
INSERT IGNORE INTO subset3_ebeb1651 (`c1`, `c2`, `c3`, `c4`, `c5`, `c6`) VALUES (6821066, 'not-a-date', 'hv_4652', 793, 'not-a-date', '2023-01-01');
INSERT IGNORE INTO subset3_ebeb1651 (`c1`, `c2`, `c3`, `c4`, `c5`, `c6`) VALUES (733752, 'not-a-date', '2023-01-01', -4, 'not-a-date', '2023-01-01');
INSERT IGNORE INTO subset3_ebeb1651 (`c1`, `c2`, `c3`, `c4`, `c5`, `c6`) VALUES (3917525, 'hv_7711', '2023-01-01', -4, '0000-00-00', '2023-01-01');
INSERT IGNORE INTO subset3_ebeb1651 (`c1`, `c2`, `c3`, `c4`, `c5`, `c6`) VALUES (7958506, '2023-01-01', 'not-a-date', 16, '0000-00-00', '2023-01-01');
INSERT IGNORE INTO subset3_ebeb1651 (`c1`, `c2`, `c3`, `c4`, `c5`, `c6`) VALUES (8650324, '01e0', 'not-a-date', -4, '0000-00-00', '2023-01-01');
INSERT IGNORE INTO subset3_ebeb1651 (`c1`, `c2`, `c3`, `c4`, `c5`, `c6`) VALUES (2732299, 'not-a-date', 'not-a-date', -4, '2023-01-01', '2023-01-01');
INSERT IGNORE INTO subset3_ebeb1651 (`c1`, `c2`, `c3`, `c4`, `c5`, `c6`) VALUES (7118831, 'not-a-date', 'hv_4652', 16, 'not-a-date', 'hv_4224');
INSERT IGNORE INTO subset3_ebeb1651 (`c1`, `c2`, `c3`, `c4`, `c5`, `c6`) VALUES (8117699, 'hv_7711', 'hv_4652', 16, '2023-01-01', '1');
INSERT IGNORE INTO subset3_ebeb1651 (`c1`, `c2`, `c3`, `c4`, `c5`, `c6`) VALUES (9349572, 'not-a-date', '2023-01-01', -6, '0000-00-00', 'hv_4224');
INSERT IGNORE INTO subset3_ebeb1651 (`c1`, `c2`, `c3`, `c4`, `c5`, `c6`) VALUES (2379627, '2023-01-01', '2023-01-01', -6, '2023-01-01', '');
INSERT IGNORE INTO subset3_ebeb1651 (`c1`, `c2`, `c3`, `c4`, `c5`, `c6`) VALUES (5665103, 'not-a-date', '2023-01-01', -6, 'not-a-date', 'hv_4224');
INSERT IGNORE INTO subset3_ebeb1651 (`c1`, `c2`, `c3`, `c4`, `c5`, `c6`) VALUES (9102434, 'hv_7711', '2023-01-01 00:00:00', 16, '2023-01-01', 'not-a-date');
INSERT IGNORE INTO subset3_ebeb1651 (`c1`, `c2`, `c3`, `c4`, `c5`, `c6`) VALUES (2586657, 'not-a-date', 'not-a-date', -4, 'not-a-date', 'not-a-date');
INSERT IGNORE INTO subset3_ebeb1651 (`c1`, `c2`, `c3`, `c4`, `c5`, `c6`) VALUES (8328309, 'not-a-date', 'not-a-date', -10, '2023-01-01', '2023-01-01');
INSERT IGNORE INTO subset3_ebeb1651 (`c1`, `c2`, `c3`, `c4`, `c5`, `c6`) VALUES (4354802, 'not-a-date', 'not-a-date', -10, 'not-a-date', 'not-a-date');
INSERT IGNORE INTO subset3_ebeb1651 (`c1`, `c2`, `c3`, `c4`, `c5`, `c6`) VALUES (4433589, 'not-a-date', 'hv_4652', -6, '2023-01-01', 'not-a-date');
INSERT IGNORE INTO subset3_ebeb1651 (`c1`, `c2`, `c3`, `c4`, `c5`, `c6`) VALUES (4878909, 'not-a-date', '2023-01-01', 16, '2023-07-10', 'not-a-date');
INSERT IGNORE INTO subset3_ebeb1651 (`c1`, `c2`, `c3`, `c4`, `c5`, `c6`) VALUES (8731782, 'z0h/m3_4njfz9-q:', 'hv_4652', 16, '0000-00-00', 'hv_4224');
INSERT IGNORE INTO subset3_ebeb1651 (`c1`, `c2`, `c3`, `c4`, `c5`, `c6`) VALUES (7500013, '2023-01-01', 'hv_4652', -10, '2023-01-01', 'not-a-date');
INSERT IGNORE INTO subset3_ebeb1651 (`c1`, `c2`, `c3`, `c4`, `c5`, `c6`) VALUES (5455347, 'hv_7711', 'not-a-date', 16, '0000-00-00', 'hv_4224');
INSERT IGNORE INTO subset3_ebeb1651 (`c1`, `c2`, `c3`, `c4`, `c5`, `c6`) VALUES (3183787, '2023-01-01', 'rfe40xuvnt4a2/g', -4, 'not-a-date', 'not-a-date');
INSERT IGNORE INTO subset3_ebeb1651 (`c1`, `c2`, `c3`, `c4`, `c5`, `c6`) VALUES (4006050, 'not-a-date', 'not-a-date', 16, '2023-01-01', 'not-a-date');
INSERT IGNORE INTO subset3_ebeb1651 (`c1`, `c2`, `c3`, `c4`, `c5`, `c6`) VALUES (6212955, '2023-01-01', 'not-a-date', 16, 'not-a-date', '2023-01-01');
INSERT IGNORE INTO subset3_ebeb1651 (`c1`, `c2`, `c3`, `c4`, `c5`, `c6`) VALUES (2287308, '2023-01-01', 'not-a-date', -10, 'not-a-date', 'not-a-date');
INSERT IGNORE INTO subset3_ebeb1651 (`c1`, `c2`, `c3`, `c4`, `c5`, `c6`) VALUES (7888852, 'not-a-date', '2023-01-01', 16, '0000-00-00', 'hv_4224');
INSERT IGNORE INTO subset3_ebeb1651 (`c1`, `c2`, `c3`, `c4`, `c5`, `c6`) VALUES (9152968, 'p4s2ak31blpxbwdfu-1u', 'not-a-date', -6, 'not-a-date', '2023-01-01');
INSERT IGNORE INTO subset3_ebeb1651 (`c1`, `c2`, `c3`, `c4`, `c5`, `c6`) VALUES (567497, 'hv_7711', 'not-a-date', 16, '0000-00-00', 'hv_4224');
INSERT IGNORE INTO subset3_ebeb1651 (`c1`, `c2`, `c3`, `c4`, `c5`, `c6`) VALUES (5758788, 'not-a-date', 'not-a-date', 16, 'not-a-date', 'not-a-date');
INSERT IGNORE INTO subset3_ebeb1651 (`c1`, `c2`, `c3`, `c4`, `c5`, `c6`) VALUES (7800676, 'hv_7711', 'pw_-feqx7t', -6, '2014-11-08', 'not-a-date');
INSERT IGNORE INTO subset3_ebeb1651 (`c1`, `c2`, `c3`, `c4`, `c5`, `c6`) VALUES (3603737, 'not-a-date', 'hv_4652', 16, '2023-01-01', 'not-a-date');
INSERT IGNORE INTO subset3_ebeb1651 (`c1`, `c2`, `c3`, `c4`, `c5`, `c6`) VALUES (6879108, 'hv_7711', 'hv_4652', 574, 'not-a-date', 'not-a-date');
INSERT IGNORE INTO subset3_ebeb1651 (`c1`, `c2`, `c3`, `c4`, `c5`, `c6`) VALUES (9462218, 'not-a-date', 'not-a-date', -10, '1994-04-03', '2023-01-01');
INSERT IGNORE INTO subset3_ebeb1651 (`c1`, `c2`, `c3`, `c4`, `c5`, `c6`) VALUES (5828022, 'not-a-date', 'hv_4652', -4, '0000-00-00', 'hv_4224');
INSERT IGNORE INTO subset3_ebeb1651 (`c1`, `c2`, `c3`, `c4`, `c5`, `c6`) VALUES (474909, '2023-01-01', '2023-01-01', -4, '0000-00-00', 'not-a-date');
INSERT IGNORE INTO subset3_ebeb1651 (`c1`, `c2`, `c3`, `c4`, `c5`, `c6`) VALUES (3426384, 'hv_7711', '2023-01-01', 16, '0000-00-00', 'not-a-date');
INSERT IGNORE INTO subset3_ebeb1651 (`c1`, `c2`, `c3`, `c4`, `c5`, `c6`) VALUES (1995436, 'not-a-date', 'hv_4652', -4, 'not-a-date', 'hv_4224');
INSERT IGNORE INTO subset3_ebeb1651 (`c1`, `c2`, `c3`, `c4`, `c5`, `c6`) VALUES (5926203, 'hv_7711', 'hv_4652', -6, '2023-01-01', '1');
INSERT IGNORE INTO subset3_ebeb1651 (`c1`, `c2`, `c3`, `c4`, `c5`, `c6`) VALUES (474393, '2023-01-01', '2023-01-01', 16, 'not-a-date', 'hv_4224');
INSERT IGNORE INTO subset3_ebeb1651 (`c1`, `c2`, `c3`, `c4`, `c5`, `c6`) VALUES (7229812, 'not-a-date', '2023-01-01', -4, '0000-00-00', 'hv_4224');
INSERT IGNORE INTO subset3_ebeb1651 (`c1`, `c2`, `c3`, `c4`, `c5`, `c6`) VALUES (2504130, '2023-01-01', 'not-a-date', 16, '2023-01-01', 'not-a-date');
INSERT IGNORE INTO subset3_ebeb1651 (`c1`, `c2`, `c3`, `c4`, `c5`, `c6`) VALUES (9870063, 'hv_7711', 'hv_4652', -10, '2023-01-01', 'hv_4224');
INSERT IGNORE INTO subset3_ebeb1651 (`c1`, `c2`, `c3`, `c4`, `c5`, `c6`) VALUES (6007057, 'not-a-date', '2023-01-01', -4, '0000-00-00', 'hv_4224');
INSERT IGNORE INTO subset3_ebeb1651 (`c1`, `c2`, `c3`, `c4`, `c5`, `c6`) VALUES (3454986, '', 'not-a-date', -6, '0000-00-00', '2023-01-01');
INSERT IGNORE INTO subset3_ebeb1651 (`c1`, `c2`, `c3`, `c4`, `c5`, `c6`) VALUES (6874545, 'hv_7711', '2023-01-01', -4, 'not-a-date', 'hv_4224');
INSERT IGNORE INTO subset3_ebeb1651 (`c1`, `c2`, `c3`, `c4`, `c5`, `c6`) VALUES (5723033, '2023-01-01', '0000-00-00', -4, '2023-01-01', 'hv_4224');
INSERT IGNORE INTO subset3_ebeb1651 (`c1`, `c2`, `c3`, `c4`, `c5`, `c6`) VALUES (2299232, '2023-01-01', 'not-a-date', 16, '0000-00-00', 'not-a-date');
INSERT IGNORE INTO subset3_ebeb1651 (`c1`, `c2`, `c3`, `c4`, `c5`, `c6`) VALUES (9282226, '2023-01-01', '2023-01-01', -10, '2023-01-01', 'hv_4224');
INSERT IGNORE INTO subset3_ebeb1651 (`c1`, `c2`, `c3`, `c4`, `c5`, `c6`) VALUES (7148283, '', 'not-a-date', -10, '2023-01-01', 'not-a-date');
INSERT IGNORE INTO subset3_ebeb1651 (`c1`, `c2`, `c3`, `c4`, `c5`, `c6`) VALUES (9331028, '2023-01-01', 'hv_4652', 16, '0000-00-00', '2023-01-01');
INSERT IGNORE INTO subset3_ebeb1651 (`c1`, `c2`, `c3`, `c4`, `c5`, `c6`) VALUES (5843870, 'hv_7711', '-1', -4, '0000-00-00', 'not-a-date');
INSERT IGNORE INTO subset3_ebeb1651 (`c1`, `c2`, `c3`, `c4`, `c5`, `c6`) VALUES (468661, '2023-01-01', 'hv_4652', 899, 'not-a-date', 'hv_4224');
INSERT IGNORE INTO subset3_ebeb1651 (`c1`, `c2`, `c3`, `c4`, `c5`, `c6`) VALUES (9747172, 'hv_7711', 'not-a-date', -646, '2023-01-01', '2023-01-01');
INSERT IGNORE INTO subset3_ebeb1651 (`c1`, `c2`, `c3`, `c4`, `c5`, `c6`) VALUES (815883, '2023-01-01', '2023-01-01', -6, '0000-00-00', 'not-a-date');
INSERT IGNORE INTO subset3_ebeb1651 (`c1`, `c2`, `c3`, `c4`, `c5`, `c6`) VALUES (7140815, '2023-01-01', '2023-01-01', 16, 'not-a-date', 'not-a-date');
INSERT IGNORE INTO subset3_ebeb1651 (`c1`, `c2`, `c3`, `c4`, `c5`, `c6`) VALUES (360777, '2023-01-01', '2023-01-01', 16, '2023-01-01', '2023-01-01');
INSERT IGNORE INTO subset3_ebeb1651 (`c1`, `c2`, `c3`, `c4`, `c5`, `c6`) VALUES (2822187, '2023-01-01', 'hv_4652', 16, 'not-a-date', '/ksi6o8os1');
INSERT IGNORE INTO subset3_ebeb1651 (`c1`, `c2`, `c3`, `c4`, `c5`, `c6`) VALUES (4202453, '2023-01-01', 'hv_4652', -6, 'not-a-date', 'hv_4224');
INSERT IGNORE INTO subset3_ebeb1651 (`c1`, `c2`, `c3`, `c4`, `c5`, `c6`) VALUES (7562880, '2023-01-01 00:00:00', 'hv_4652', 16, '2023-01-01', 'hv_4224');
INSERT IGNORE INTO subset3_ebeb1651 (`c1`, `c2`, `c3`, `c4`, `c5`, `c6`) VALUES (1138775, '2023-01-01', 'not-a-date', -6, '2023-01-01', 'hv_4224');
INSERT IGNORE INTO subset3_ebeb1651 (`c1`, `c2`, `c3`, `c4`, `c5`, `c6`) VALUES (2072591, 'not-a-date', 'hv_4652', -10, 'not-a-date', '2023-01-01');
INSERT IGNORE INTO subset3_ebeb1651 (`c1`, `c2`, `c3`, `c4`, `c5`, `c6`) VALUES (1557916, 'hv_7711', 'bb7tbnkq3qla', -866, '0000-00-00', 'hv_4224');
INSERT IGNORE INTO subset3_ebeb1651 (`c1`, `c2`, `c3`, `c4`, `c5`, `c6`) VALUES (9725517, '2023-01-01', 'hv_4652', 16, 'not-a-date', 'not-a-date');
INSERT IGNORE INTO subset3_ebeb1651 (`c1`, `c2`, `c3`, `c4`, `c5`, `c6`) VALUES (6922157, 'hv_7711', '2023-01-01', 630, 'not-a-date', '2023-01-01');
INSERT IGNORE INTO subset3_ebeb1651 (`c1`, `c2`, `c3`, `c4`, `c5`, `c6`) VALUES (7847205, 'not-a-date', '2023-01-01', 16, 'not-a-date', 'not-a-date');
INSERT IGNORE INTO subset3_ebeb1651 (`c1`, `c2`, `c3`, `c4`, `c5`, `c6`) VALUES (8539527, 'hv_7711', '2023-01-01', -4, '0000-00-00', '2023-01-01');
INSERT IGNORE INTO subset3_ebeb1651 (`c1`, `c2`, `c3`, `c4`, `c5`, `c6`) VALUES (7435003, '2023-01-01', '2023-01-01', -985, '0000-00-00', 'hv_4224');
INSERT IGNORE INTO subset3_ebeb1651 (`c1`, `c2`, `c3`, `c4`, `c5`, `c6`) VALUES (4897089, '2023-01-01', '2023-01-01', -4, 'not-a-date', '2023-01-01');
INSERT IGNORE INTO subset3_ebeb1651 (`c1`, `c2`, `c3`, `c4`, `c5`, `c6`) VALUES (9298831, '2023-01-01', 'hv_4652', 16, 'not-a-date', 'not-a-date');
INSERT IGNORE INTO subset3_ebeb1651 (`c1`, `c2`, `c3`, `c4`, `c5`, `c6`) VALUES (9878683, 'hv_7711', 'hv_4652', 16, 'not-a-date', 'not-a-date');
INSERT IGNORE INTO subset3_ebeb1651 (`c1`, `c2`, `c3`, `c4`, `c5`, `c6`) VALUES (5223632, 'hv_7711', 'hv_4652', -10, '2023-01-01', 'hv_4224');
INSERT IGNORE INTO subset3_ebeb1651 (`c1`, `c2`, `c3`, `c4`, `c5`, `c6`) VALUES (1933610, '2023-01-01', 'hv_4652', 16, 'not-a-date', 'not-a-date');
INSERT IGNORE INTO subset3_ebeb1651 (`c1`, `c2`, `c3`, `c4`, `c5`, `c6`) VALUES (1406866, 'hv_7711', '2023-01-01', -10, '2023-01-01', 'not-a-date');
INSERT IGNORE INTO subset3_ebeb1651 (`c1`, `c2`, `c3`, `c4`, `c5`, `c6`) VALUES (6050823, 'hv_7711', '2023-01-01', 16, '2023-01-01', 'hv_4224');
INSERT IGNORE INTO subset3_ebeb1651 (`c1`, `c2`, `c3`, `c4`, `c5`, `c6`) VALUES (5758680, 'q-3a-vpnnx7dur', 'hv_4652', -4, 'not-a-date', 'hv_4224');
INSERT IGNORE INTO subset3_ebeb1651 (`c1`, `c2`, `c3`, `c4`, `c5`, `c6`) VALUES (3630918, 'not-a-date', '2023-01-01', -10, '2037-12-21', '1');
INSERT IGNORE INTO subset3_ebeb1651 (`c1`, `c2`, `c3`, `c4`, `c5`, `c6`) VALUES (6603418, 'hv_7711', 'not-a-date', -6, 'not-a-date', 'hv_4224');
INSERT IGNORE INTO subset3_ebeb1651 (`c1`, `c2`, `c3`, `c4`, `c5`, `c6`) VALUES (1002342, 'hv_7711', 'not-a-date', -10, '2023-01-01', 'not-a-date');
INSERT IGNORE INTO subset3_ebeb1651 (`c1`, `c2`, `c3`, `c4`, `c5`, `c6`) VALUES (3128164, 'not-a-date', 'not-a-date', 16, '2023-01-01', 'hv_4224');
INSERT IGNORE INTO subset3_ebeb1651 (`c1`, `c2`, `c3`, `c4`, `c5`, `c6`) VALUES (4325397, 'not-a-date', '2023-01-01', 16, '0000-00-00', '2023-01-01 00:00:00');
INSERT IGNORE INTO subset3_ebeb1651 (`c1`, `c2`, `c3`, `c4`, `c5`, `c6`) VALUES (4572079, 'not-a-date', '2023-01-01', 16, '0000-00-00', 'not-a-date');
INSERT IGNORE INTO subset3_ebeb1651 (`c1`, `c2`, `c3`, `c4`, `c5`, `c6`) VALUES (1905485, 'not-a-date', 'hv_4652', 16, 'not-a-date', 'not-a-date');
INSERT IGNORE INTO subset3_ebeb1651 (`c1`, `c2`, `c3`, `c4`, `c5`, `c6`) VALUES (5022762, 'hv_7711', '2023-01-01', -4, 'not-a-date', 'not-a-date');
INSERT IGNORE INTO subset3_ebeb1651 (`c1`, `c2`, `c3`, `c4`, `c5`, `c6`) VALUES (6822504, 'not-a-date', 'hv_4652', 16, '2023-01-01', 'hv_4224');
INSERT IGNORE INTO subset3_ebeb1651 (`c1`, `c2`, `c3`, `c4`, `c5`, `c6`) VALUES (8988301, 'not-a-date', 'hv_4652', -10, 'not-a-date', 'hv_4224');
INSERT IGNORE INTO subset3_ebeb1651 (`c1`, `c2`, `c3`, `c4`, `c5`, `c6`) VALUES (3504227, 'hv_7711', 'not-a-date', 16, 'not-a-date', 'hv_4224');
INSERT IGNORE INTO subset3_ebeb1651 (`c1`, `c2`, `c3`, `c4`, `c5`, `c6`) VALUES (1194582, 'not-a-date', 'hv_4652', -4, 'not-a-date', 'hv_4224');
INSERT IGNORE INTO subset3_ebeb1651 (`c1`, `c2`, `c3`, `c4`, `c5`, `c6`) VALUES (8364747, '2023-01-01', '1', 16, '2023-01-01', 'hv_4224');
INSERT IGNORE INTO subset3_ebeb1651 (`c1`, `c2`, `c3`, `c4`, `c5`, `c6`) VALUES (5007891, 'hv_7711', 'not-a-date', 16, 'not-a-date', '2023-01-01');
INSERT IGNORE INTO subset3_ebeb1651 (`c1`, `c2`, `c3`, `c4`, `c5`, `c6`) VALUES (8737653, 'not-a-date', 'hv_4652', -6, 'not-a-date', 'not-a-date');
INSERT IGNORE INTO subset3_ebeb1651 (`c1`, `c2`, `c3`, `c4`, `c5`, `c6`) VALUES (9995584, '6ncfvn31_jc', 'not-a-date', -6, '0000-00-00', 'hv_4224');
INSERT IGNORE INTO subset3_ebeb1651 (`c1`, `c2`, `c3`, `c4`, `c5`, `c6`) VALUES (8025588, '2023-01-01', 'hv_4652', 16, 'not-a-date', 'not-a-date');
INSERT IGNORE INTO subset3_ebeb1651 (`c1`, `c2`, `c3`, `c4`, `c5`, `c6`) VALUES (8594489, '2023-01-01', 'not-a-date', 16, '0000-00-00', '2023-01-01');
INSERT IGNORE INTO subset3_ebeb1651 (`c1`, `c2`, `c3`, `c4`, `c5`, `c6`) VALUES (2008628, '2023-01-01', 'hv_4652', -10, '2023-01-01', '2023-01-01');
INSERT IGNORE INTO subset3_ebeb1651 (`c1`, `c2`, `c3`, `c4`, `c5`, `c6`) VALUES (6884694, 'hv_7711', '2023-01-01', -6, 'not-a-date', 'not-a-date');
INSERT IGNORE INTO subset3_ebeb1651 (`c1`, `c2`, `c3`, `c4`, `c5`, `c6`) VALUES (4810263, 'not-a-date', '2023-01-01', -10, 'not-a-date', 'hv_4224');
INSERT IGNORE INTO subset3_ebeb1651 (`c1`, `c2`, `c3`, `c4`, `c5`, `c6`) VALUES (1857844, 'hv_7711', '2023-01-01', 16, '2019-11-19', 'hv_4224');
INSERT IGNORE INTO subset3_ebeb1651 (`c1`, `c2`, `c3`, `c4`, `c5`, `c6`) VALUES (2974978, 'not-a-date', 'not-a-date', -6, '0000-00-00', '2023-01-01');
INSERT IGNORE INTO subset3_ebeb1651 (`c1`, `c2`, `c3`, `c4`, `c5`, `c6`) VALUES (174045, '2023-01-01', 'hv_4652', -4, '2023-01-01', '01e0');
INSERT IGNORE INTO subset3_ebeb1651 (`c1`, `c2`, `c3`, `c4`, `c5`, `c6`) VALUES (4659381, 'not-a-date', 'not-a-date', 16, '2023-01-01', '2023-01-01');
INSERT IGNORE INTO subset3_ebeb1651 (`c1`, `c2`, `c3`, `c4`, `c5`, `c6`) VALUES (9202472, 'not-a-date', '2023-01-01', 16, '0000-00-00', '2023-01-01');
INSERT IGNORE INTO subset3_ebeb1651 (`c1`, `c2`, `c3`, `c4`, `c5`, `c6`) VALUES (3491743, '2023-01-01', 'hv_4652', 16, '2023-01-01', '2023-01-01');
INSERT IGNORE INTO subset3_ebeb1651 (`c1`, `c2`, `c3`, `c4`, `c5`, `c6`) VALUES (5437400, '2023-01-01', 'not-a-date', 16, '0000-00-00', '2023-01-01');
INSERT IGNORE INTO subset3_ebeb1651 (`c1`, `c2`, `c3`, `c4`, `c5`, `c6`) VALUES (8233072, 'hv_7711', 'hv_4652', -4, 'not-a-date', '2023-01-01');
INSERT IGNORE INTO subset3_ebeb1651 (`c1`, `c2`, `c3`, `c4`, `c5`, `c6`) VALUES (2966592, 'not-a-date', 'not-a-date', -10, '2023-01-01', 'hv_4224');
INSERT IGNORE INTO subset3_ebeb1651 (`c1`, `c2`, `c3`, `c4`, `c5`, `c6`) VALUES (4599444, 'not-a-date', '2023-01-01', -6, '2023-01-01', '2023-01-01');
INSERT IGNORE INTO subset3_ebeb1651 (`c1`, `c2`, `c3`, `c4`, `c5`, `c6`) VALUES (3178838, '2023-01-01', '2023-01-01', -6, '2023-01-01', 'not-a-date');
INSERT IGNORE INTO subset3_ebeb1651 (`c1`, `c2`, `c3`, `c4`, `c5`, `c6`) VALUES (3902118, 'hv_7711', 'hv_4652', -6, '0000-00-00', 'not-a-date');
INSERT IGNORE INTO subset3_ebeb1651 (`c1`, `c2`, `c3`, `c4`, `c5`, `c6`) VALUES (5308444, 'hv_7711', 'not-a-date', -10, '2023-01-01', '1');
INSERT IGNORE INTO subset3_ebeb1651 (`c1`, `c2`, `c3`, `c4`, `c5`, `c6`) VALUES (4491443, '2023-01-01', '2023-01-01', 16, '2023-01-01', 'not-a-date');
INSERT IGNORE INTO subset3_ebeb1651 (`c1`, `c2`, `c3`, `c4`, `c5`, `c6`) VALUES (7405896, 'hv_7711', '8fwu_5vu/g', 16, '0000-00-00', '2023-01-01');
INSERT IGNORE INTO subset3_ebeb1651 (`c1`, `c2`, `c3`, `c4`, `c5`, `c6`) VALUES (6692306, '2023-01-01', 'hv_4652', -10, '0000-00-00', '2023-01-01');
INSERT IGNORE INTO subset3_ebeb1651 (`c1`, `c2`, `c3`, `c4`, `c5`, `c6`) VALUES (6985119, 'hv_7711', 'hv_4652', 16, '2038-03-15', 'hv_4224');
INSERT IGNORE INTO subset3_ebeb1651 (`c1`, `c2`, `c3`, `c4`, `c5`, `c6`) VALUES (4818359, '2023-01-01', 'not-a-date', 16, '2023-01-01', 'not-a-date');
INSERT IGNORE INTO subset3_ebeb1651 (`c1`, `c2`, `c3`, `c4`, `c5`, `c6`) VALUES (384930, 'not-a-date', 'not-a-date', 16, '0000-00-00', 'hv_4224');
INSERT IGNORE INTO subset3_ebeb1651 (`c1`, `c2`, `c3`, `c4`, `c5`, `c6`) VALUES (8200477, '2023-01-01', '2023-01-01', 318, 'not-a-date', '2023-01-01');
INSERT IGNORE INTO subset3_ebeb1651 (`c1`, `c2`, `c3`, `c4`, `c5`, `c6`) VALUES (6688747, 'not-a-date', '2023-01-01', -4, '2023-01-01', '2023-01-01');
INSERT IGNORE INTO subset3_ebeb1651 (`c1`, `c2`, `c3`, `c4`, `c5`, `c6`) VALUES (3086642, 'not-a-date', '2023-01-01', 16, '2023-01-01', '2023-01-01');
INSERT IGNORE INTO subset3_ebeb1651 (`c1`, `c2`, `c3`, `c4`, `c5`, `c6`) VALUES (4298579, 'not-a-date', 'hv_4652', 16, '2005-09-05', '2023-01-01');
INSERT IGNORE INTO subset3_ebeb1651 (`c1`, `c2`, `c3`, `c4`, `c5`, `c6`) VALUES (7783938, 'a:h_//782', '1sak0bxe', -6, '0000-00-00', '2023-01-01');
INSERT IGNORE INTO subset3_ebeb1651 (`c1`, `c2`, `c3`, `c4`, `c5`, `c6`) VALUES (4429901, '2023-01-01', '2023-01-01', -4, '2023-01-01', '-1');
INSERT IGNORE INTO subset3_ebeb1651 (`c1`, `c2`, `c3`, `c4`, `c5`, `c6`) VALUES (4666892, 'not-a-date', 'not-a-date', -10, 'not-a-date', '2023-01-01');
INSERT IGNORE INTO subset3_ebeb1651 (`c1`, `c2`, `c3`, `c4`, `c5`, `c6`) VALUES (8911897, 'not-a-date', 'not-a-date', -6, 'not-a-date', 'not-a-date');
INSERT IGNORE INTO subset3_ebeb1651 (`c1`, `c2`, `c3`, `c4`, `c5`, `c6`) VALUES (7807456, 'hv_7711', 'hv_4652', -6, 'not-a-date', '2023-01-01');
INSERT IGNORE INTO subset3_ebeb1651 (`c1`, `c2`, `c3`, `c4`, `c5`, `c6`) VALUES (2060180, '2023-01-01', '01e0', 16, '2023-01-01', '2023-01-01');
INSERT IGNORE INTO subset3_ebeb1651 (`c1`, `c2`, `c3`, `c4`, `c5`, `c6`) VALUES (7402821, 'hv_7711', 'not-a-date', -10, '2023-01-01', 'hv_4224');
INSERT IGNORE INTO subset3_ebeb1651 (`c1`, `c2`, `c3`, `c4`, `c5`, `c6`) VALUES (1585475, '1', 'hv_4652', -6, '2024-04-25', 'not-a-date');
INSERT IGNORE INTO subset3_ebeb1651 (`c1`, `c2`, `c3`, `c4`, `c5`, `c6`) VALUES (2916019, 'not-a-date', '2023-01-01', -6, 'not-a-date', 'hv_4224');
INSERT IGNORE INTO subset3_ebeb1651 (`c1`, `c2`, `c3`, `c4`, `c5`, `c6`) VALUES (2499032, '2023-01-01', 'hv_4652', -10, '2023-01-01', '2023-01-01');
INSERT IGNORE INTO subset3_ebeb1651 (`c1`, `c2`, `c3`, `c4`, `c5`, `c6`) VALUES (3079047, 'hv_7711', 'hv_4652', 16, '0000-00-00', '2023-01-01');
INSERT IGNORE INTO subset3_ebeb1651 (`c1`, `c2`, `c3`, `c4`, `c5`, `c6`) VALUES (1002948, '2023-01-01', ' zhyrthy7omy8vt', -4, 'not-a-date', 'hv_4224');
INSERT IGNORE INTO subset3_ebeb1651 (`c1`, `c2`, `c3`, `c4`, `c5`, `c6`) VALUES (7138190, 'hv_7711', 'not-a-date', -6, 'not-a-date', '2023-01-01');
INSERT IGNORE INTO subset3_ebeb1651 (`c1`, `c2`, `c3`, `c4`, `c5`, `c6`) VALUES (4405117, '2023-01-01', 'hv_4652', 16, '0000-00-00', 'not-a-date');
INSERT IGNORE INTO subset3_ebeb1651 (`c1`, `c2`, `c3`, `c4`, `c5`, `c6`) VALUES (1483067, 'not-a-date', '2023-01-01', -6, '2023-01-01', '0');
INSERT IGNORE INTO subset3_ebeb1651 (`c1`, `c2`, `c3`, `c4`, `c5`, `c6`) VALUES (8019162, 'hv_7711', '2023-01-01', -4, '2037-02-17', 'hv_4224');
INSERT IGNORE INTO subset3_ebeb1651 (`c1`, `c2`, `c3`, `c4`, `c5`, `c6`) VALUES (8423499, 'not-a-date', 'not-a-date', -6, '2023-09-26', 'hv_4224');
INSERT IGNORE INTO subset3_ebeb1651 (`c1`, `c2`, `c3`, `c4`, `c5`, `c6`) VALUES (48954, 'not-a-date', 'not-a-date', 16, '0000-00-00', 'not-a-date');
INSERT IGNORE INTO subset3_ebeb1651 (`c1`, `c2`, `c3`, `c4`, `c5`, `c6`) VALUES (5034700, 'not-a-date', 'not-a-date', 16, '2023-01-01', 'hv_4224');
INSERT IGNORE INTO subset3_ebeb1651 (`c1`, `c2`, `c3`, `c4`, `c5`, `c6`) VALUES (7130278, 'hv_7711', 'not-a-date', 16, '2023-01-01', 'hv_4224');
INSERT IGNORE INTO subset3_ebeb1651 (`c1`, `c2`, `c3`, `c4`, `c5`, `c6`) VALUES (4210070, 'hv_7711', 'not-a-date', 16, '0000-00-00', '7twlgu-dwe2');
INSERT IGNORE INTO subset3_ebeb1651 (`c1`, `c2`, `c3`, `c4`, `c5`, `c6`) VALUES (1760732, 'not-a-date', '2023-01-01', 16, '2015-05-08', 'not-a-date');
INSERT IGNORE INTO subset3_ebeb1651 (`c1`, `c2`, `c3`, `c4`, `c5`, `c6`) VALUES (8978851, '2023-01-01', '2023-01-01', -10, '2022-10-18', 'not-a-date');
INSERT IGNORE INTO subset3_ebeb1651 (`c1`, `c2`, `c3`, `c4`, `c5`, `c6`) VALUES (7521652, 'not-a-date', 'not-a-date', -10, '2032-04-27', '2023-01-01');
INSERT IGNORE INTO subset3_ebeb1651 (`c1`, `c2`, `c3`, `c4`, `c5`, `c6`) VALUES (4059063, '2023-01-01', '2023-01-01', -10, '0000-00-00', '2023-01-01');
INSERT IGNORE INTO subset3_ebeb1651 (`c1`, `c2`, `c3`, `c4`, `c5`, `c6`) VALUES (6923842, 'not-a-date', 'hv_4652', 649, '0000-00-00', 'hv_4224');
INSERT IGNORE INTO subset3_ebeb1651 (`c1`, `c2`, `c3`, `c4`, `c5`, `c6`) VALUES (6356226, 'hv_7711', '2023-01-01', -4, '2023-01-01', 'hv_4224');
INSERT IGNORE INTO subset3_ebeb1651 (`c1`, `c2`, `c3`, `c4`, `c5`, `c6`) VALUES (3164679, 'not-a-date', '2023-01-01', -10, '0000-00-00', '2023-01-01');
INSERT IGNORE INTO subset3_ebeb1651 (`c1`, `c2`, `c3`, `c4`, `c5`, `c6`) VALUES (9846640, '2023-01-01', '2023-01-01', -6, '0000-00-00', '2023-01-01');
INSERT IGNORE INTO subset3_ebeb1651 (`c1`, `c2`, `c3`, `c4`, `c5`, `c6`) VALUES (5572488, 'not-a-date', 'not-a-date', 16, '2023-01-01', 'hv_4224');
INSERT IGNORE INTO subset3_ebeb1651 (`c1`, `c2`, `c3`, `c4`, `c5`, `c6`) VALUES (1043726, 'hv_7711', 'not-a-date', -6, '0000-00-00', 'not-a-date');
INSERT IGNORE INTO subset3_ebeb1651 (`c1`, `c2`, `c3`, `c4`, `c5`, `c6`) VALUES (8818500, '2023-01-01', 'not-a-date', -10, 'not-a-date', 'rkd k4e1v28s8q_0en');
INSERT IGNORE INTO subset3_ebeb1651 (`c1`, `c2`, `c3`, `c4`, `c5`, `c6`) VALUES (9089315, '2023-01-01', '', 16, 'not-a-date', 'hv_4224');
INSERT IGNORE INTO subset3_ebeb1651 (`c1`, `c2`, `c3`, `c4`, `c5`, `c6`) VALUES (1726809, 'not-a-date', '2023-01-01', 16, 'not-a-date', 'hv_4224');
INSERT IGNORE INTO subset3_ebeb1651 (`c1`, `c2`, `c3`, `c4`, `c5`, `c6`) VALUES (142263, '-523e7uy8pun', 'not-a-date', -6, '2023-01-01', 'not-a-date');
INSERT IGNORE INTO subset3_ebeb1651 (`c1`, `c2`, `c3`, `c4`, `c5`, `c6`) VALUES (1163505, 'hv_7711', 'not-a-date', 16, 'not-a-date', 'not-a-date');
INSERT IGNORE INTO subset3_ebeb1651 (`c1`, `c2`, `c3`, `c4`, `c5`, `c6`) VALUES (1339485, 'hv_7711', 'hv_4652', -10, '1991-08-16', '2023-01-01');
INSERT IGNORE INTO subset3_ebeb1651 (`c1`, `c2`, `c3`, `c4`, `c5`, `c6`) VALUES (3350862, 'not-a-date', 'not-a-date', 16, 'not-a-date', 'hv_4224');
INSERT IGNORE INTO subset3_ebeb1651 (`c1`, `c2`, `c3`, `c4`, `c5`, `c6`) VALUES (8371847, 'hv_7711', '2023-01-01', -10, '0000-00-00', 'not-a-date');
INSERT IGNORE INTO subset3_ebeb1651 (`c1`, `c2`, `c3`, `c4`, `c5`, `c6`) VALUES (6991348, 'hv_7711', 'hv_4652', 16, '0000-00-00', 'not-a-date');
INSERT IGNORE INTO subset3_ebeb1651 (`c1`, `c2`, `c3`, `c4`, `c5`, `c6`) VALUES (8981503, 'not-a-date', '2023-01-01', -4, '0000-00-00', 'not-a-date');
INSERT IGNORE INTO subset3_ebeb1651 (`c1`, `c2`, `c3`, `c4`, `c5`, `c6`) VALUES (5954925, '', 'hv_4652', -10, '2023-01-01', 'hv_4224');
INSERT IGNORE INTO subset3_ebeb1651 (`c1`, `c2`, `c3`, `c4`, `c5`, `c6`) VALUES (1240833, 'not-a-date', '0', -6, '2023-01-01', '2023-01-01');
INSERT IGNORE INTO subset3_ebeb1651 (`c1`, `c2`, `c3`, `c4`, `c5`, `c6`) VALUES (2599537, '2023-01-01', 'hv_4652', -10, '2000-07-26', '2023-01-01');
INSERT IGNORE INTO subset3_ebeb1651 (`c1`, `c2`, `c3`, `c4`, `c5`, `c6`) VALUES (4463876, 'not-a-date', '2023-01-01', -10, '2023-01-01', '1');
INSERT IGNORE INTO subset3_ebeb1651 (`c1`, `c2`, `c3`, `c4`, `c5`, `c6`) VALUES (1573063, 'not-a-date', 'not-a-date', 16, 'not-a-date', '2023-01-01');
INSERT IGNORE INTO subset3_ebeb1651 (`c1`, `c2`, `c3`, `c4`, `c5`, `c6`) VALUES (1717229, '2023-01-01', 'hv_4652', 16, '2023-01-01', 'not-a-date');
INSERT IGNORE INTO subset3_ebeb1651 (`c1`, `c2`, `c3`, `c4`, `c5`, `c6`) VALUES (2938272, 'hv_7711', '2023-01-01', 16, '2023-01-01', '2023-01-01');
INSERT IGNORE INTO subset3_ebeb1651 (`c1`, `c2`, `c3`, `c4`, `c5`, `c6`) VALUES (6846073, 'not-a-date', 'hv_4652', -10, 'not-a-date', 'hv_4224');
INSERT IGNORE INTO subset3_ebeb1651 (`c1`, `c2`, `c3`, `c4`, `c5`, `c6`) VALUES (5760302, ' 1', '1', -6, '2023-01-01', 'hv_4224');
INSERT IGNORE INTO subset3_ebeb1651 (`c1`, `c2`, `c3`, `c4`, `c5`, `c6`) VALUES (2902167, 'not-a-date', 'not-a-date', -6, 'not-a-date', 'ke275/qjbs');
INSERT IGNORE INTO subset3_ebeb1651 (`c1`, `c2`, `c3`, `c4`, `c5`, `c6`) VALUES (6834121, 'hv_7711', '2023-01-01', -6, '0000-00-00', 'hv_4224');
INSERT IGNORE INTO subset3_ebeb1651 (`c1`, `c2`, `c3`, `c4`, `c5`, `c6`) VALUES (8725172, 'not-a-date', '2023-01-01', -6, '2023-01-01', 'not-a-date');
INSERT IGNORE INTO subset3_ebeb1651 (`c1`, `c2`, `c3`, `c4`, `c5`, `c6`) VALUES (9848772, 'hv_7711', 'hv_4652', 16, '2023-01-01', '2023-01-01');
INSERT IGNORE INTO subset3_ebeb1651 (`c1`, `c2`, `c3`, `c4`, `c5`, `c6`) VALUES (6483181, 'not-a-date', 'hv_4652', -10, 'not-a-date', 'hv_4224');
INSERT IGNORE INTO subset3_ebeb1651 (`c1`, `c2`, `c3`, `c4`, `c5`, `c6`) VALUES (8055241, 'not-a-date', 'not-a-date', -4, '0000-00-00', 'not-a-date');
INSERT IGNORE INTO subset3_ebeb1651 (`c1`, `c2`, `c3`, `c4`, `c5`, `c6`) VALUES (229499, 'hv_7711', 'not-a-date', 16, 'not-a-date', '2023-01-01');
INSERT IGNORE INTO subset3_ebeb1651 (`c1`, `c2`, `c3`, `c4`, `c5`, `c6`) VALUES (7204851, 'not-a-date', '2023-01-01', 16, 'not-a-date', 'hv_4224');
INSERT IGNORE INTO subset3_ebeb1651 (`c1`, `c2`, `c3`, `c4`, `c5`, `c6`) VALUES (8849080, '2023-01-01', 'not-a-date', -10, '0000-00-00', 'hv_4224');
INSERT IGNORE INTO subset3_ebeb1651 (`c1`, `c2`, `c3`, `c4`, `c5`, `c6`) VALUES (3078615, '0', ' riuyf1 m', -355, '1996-06-24', '2023-01-01');
INSERT IGNORE INTO subset3_ebeb1651 (`c1`, `c2`, `c3`, `c4`, `c5`, `c6`) VALUES (8825250, 'not-a-date', 'hv_4652', -6, '0000-00-00', '-i9mgxrkeaok570');
COMMIT;
ANALYZE TABLE subset3_ebeb1651;

-- verify query --
SELECT (COALESCE(`sub1`.`c4`, 0) + COALESCE(`sub1`.`c6`, 0)), CONCAT(COALESCE(`sub1`.`c2`, ''), COALESCE(`sub1`.`c4`, '')), (CASE WHEN `sub1`.`c3` IS NULL THEN 'missing' ELSE `sub1`.`c3` END), UPPER(`sub1`.`c6`) FROM `subset3_ebeb1651` sub1 WHERE (`sub1`.`c5` IS NOT NULL OR `sub1`.`c6` >= '2023-01-01') AND `sub1`.`c4` = -6 AND EXISTS (SELECT 1 FROM `subset3_ref_ebeb1651_t3` ex2 WHERE `ex2`.`c4` = `ex2`.`c10` AND `sub1`.`c2` = `ex2`.`c4`) ORDER BY `sub1`.`c6` DESC;
EXPLAIN FORMAT=TRADITIONAL SELECT (COALESCE(`sub1`.`c4`, 0) + COALESCE(`sub1`.`c6`, 0)), CONCAT(COALESCE(`sub1`.`c2`, ''), COALESCE(`sub1`.`c4`, '')), (CASE WHEN `sub1`.`c3` IS NULL THEN 'missing' ELSE `sub1`.`c3` END), UPPER(`sub1`.`c6`) FROM `subset3_ebeb1651` sub1 WHERE (`sub1`.`c5` IS NOT NULL OR `sub1`.`c6` >= '2023-01-01') AND `sub1`.`c4` = -6 AND EXISTS (SELECT 1 FROM `subset3_ref_ebeb1651_t3` ex2 WHERE `ex2`.`c4` = `ex2`.`c10` AND `sub1`.`c2` = `ex2`.`c4`) ORDER BY `sub1`.`c6` DESC;
ANALYZE TABLE subset3_ebeb1651, subset3_ref_ebeb1651_t3;

-- Extra Diagnosis:
SELECT
    'witness' AS stage,
    ex2.c1,
    ex2.c4,
    ex2.c10,
    (ex2.c4 = ex2.c10) AS eq_ok
FROM subset3_ref_ebeb1651_t3 ex2
WHERE ex2.c4 = ex2.c10
ORDER BY ex2.c1
LIMIT 5;

-- 1. S1: force FirstMatch semijoin
SET SESSION optimizer_switch =
  'semijoin=on,firstmatch=on,materialization=off,loosescan=off,duplicateweedout=off';

EXPLAIN FORMAT=TRADITIONAL
SELECT sub1.c1
FROM subset3_ebeb1651 sub1
WHERE (sub1.c5 IS NOT NULL OR sub1.c6 >= '2023-01-01')
  AND sub1.c4 = -6
  AND EXISTS (
      SELECT 1
      FROM subset3_ref_ebeb1651_t3 ex2
      WHERE ex2.c4 = ex2.c10
        AND sub1.c2 = ex2.c4
  )
ORDER BY sub1.c1;

SELECT 'S1_count' AS stage, COUNT(*) AS cnt
FROM subset3_ebeb1651 sub1
WHERE (sub1.c5 IS NOT NULL OR sub1.c6 >= '2023-01-01')
  AND sub1.c4 = -6
  AND EXISTS (
      SELECT 1
      FROM subset3_ref_ebeb1651_t3 ex2
      WHERE ex2.c4 = ex2.c10
        AND sub1.c2 = ex2.c4
  );

SELECT
    'S1_sample' AS stage,
    sub1.c1,
    sub1.c2,
    sub1.c6
FROM subset3_ebeb1651 sub1
WHERE (sub1.c5 IS NOT NULL OR sub1.c6 >= '2023-01-01')
  AND sub1.c4 = -6
  AND EXISTS (
      SELECT 1
      FROM subset3_ref_ebeb1651_t3 ex2
      WHERE ex2.c4 = ex2.c10
        AND sub1.c2 = ex2.c4
  )
ORDER BY sub1.c1
LIMIT 10;

-- 2. S2: force materialized semijoin, no ORDER BY
SET SESSION optimizer_switch =
  'semijoin=on,firstmatch=off,materialization=on,loosescan=off,duplicateweedout=off';

EXPLAIN FORMAT=TRADITIONAL
SELECT sub1.c1
FROM subset3_ebeb1651 sub1
WHERE (sub1.c5 IS NOT NULL OR sub1.c6 >= '2023-01-01')
  AND sub1.c4 = -6
  AND EXISTS (
      SELECT 1
      FROM subset3_ref_ebeb1651_t3 ex2
      WHERE ex2.c4 = ex2.c10
        AND sub1.c2 = ex2.c4
  );

SELECT 'S2_no_order_count' AS stage, COUNT(*) AS cnt
FROM subset3_ebeb1651 sub1
WHERE (sub1.c5 IS NOT NULL OR sub1.c6 >= '2023-01-01')
  AND sub1.c4 = -6
  AND EXISTS (
      SELECT 1
      FROM subset3_ref_ebeb1651_t3 ex2
      WHERE ex2.c4 = ex2.c10
        AND sub1.c2 = ex2.c4
  );

SELECT
    'S2_no_order_sample' AS stage,
    sub1.c1,
    sub1.c2,
    sub1.c6
FROM subset3_ebeb1651 sub1
WHERE (sub1.c5 IS NOT NULL OR sub1.c6 >= '2023-01-01')
  AND sub1.c4 = -6
  AND EXISTS (
      SELECT 1
      FROM subset3_ref_ebeb1651_t3 ex2
      WHERE ex2.c4 = ex2.c10
        AND sub1.c2 = ex2.c4
  )
LIMIT 10;

-- 3. Same S2 family, but add ORDER BY: bug path
EXPLAIN FORMAT=TRADITIONAL
SELECT sub1.c1
FROM subset3_ebeb1651 sub1
WHERE (sub1.c5 IS NOT NULL OR sub1.c6 >= '2023-01-01')
  AND sub1.c4 = -6
  AND EXISTS (
      SELECT 1
      FROM subset3_ref_ebeb1651_t3 ex2
      WHERE ex2.c4 = ex2.c10
        AND sub1.c2 = ex2.c4
  )
ORDER BY sub1.c1;

SELECT
    'S2_with_order_rows' AS stage,
    sub1.c1,
    sub1.c2,
    sub1.c6
FROM subset3_ebeb1651 sub1
WHERE (sub1.c5 IS NOT NULL OR sub1.c6 >= '2023-01-01')
  AND sub1.c4 = -6
  AND EXISTS (
      SELECT 1
      FROM subset3_ref_ebeb1651_t3 ex2
      WHERE ex2.c4 = ex2.c10
        AND sub1.c2 = ex2.c4
  )
ORDER BY sub1.c1
LIMIT 10;