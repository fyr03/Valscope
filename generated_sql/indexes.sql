USE test;
CREATE UNIQUE INDEX idx_t1_pk ON t1 (c1);

CREATE  INDEX idx_t1_c6 ON t1 (c6);

CREATE  INDEX idx_t1_c2_c5_c3 ON t1 (c2(63), c5, c3(63));

CREATE  INDEX idx_t1_c5_c3 ON t1 (c5, c3(63));

CREATE UNIQUE INDEX idx_t2_pk ON t2 (c1);

CREATE  INDEX idx_t2_c14 ON t2 (c14);

CREATE  INDEX idx_t2_c4 ON t2 (c4);

CREATE  INDEX idx_t2_c10_c2 ON t2 (c10, c2);

CREATE UNIQUE INDEX idx_t3_pk ON t3 (c1);

CREATE  INDEX idx_t3_c5 ON t3 (c5);

CREATE  INDEX idx_t3_c14 ON t3 (c14);

CREATE  INDEX idx_t3_c8_c6_c2 ON t3 (c8, c6, c2);USE test;
CREATE UNIQUE INDEX idx_t1_pk ON t1 (c1);

CREATE  INDEX idx_t1_c2 ON t1 (c2(63));

CREATE  INDEX idx_t1_c6_c2 ON t1 (c6, c2(63));

CREATE  INDEX idx_t1_c6_c5 ON t1 (c6, c5);

CREATE UNIQUE INDEX idx_t1_pk_c6 ON t1 (c1, c6);

CREATE UNIQUE INDEX idx_t2_pk ON t2 (c1);

CREATE  INDEX idx_t2_c11 ON t2 (c11);

CREATE  INDEX idx_t2_c4 ON t2 (c4);

CREATE  INDEX idx_t2_c6_c11_c12 ON t2 (c6(50), c11, c12);

CREATE  INDEX idx_t2_c12_c7_c3 ON t2 (c12, c7(50), c3);

CREATE UNIQUE INDEX idx_t3_pk ON t3 (c1);

CREATE  INDEX idx_t3_c12 ON t3 (c12(50));

CREATE  INDEX idx_t3_c9 ON t3 (c9);

CREATE  INDEX idx_t3_c14_c3 ON t3 (c14, c3);USE test;
CREATE UNIQUE INDEX idx_t1_pk ON t1 (c1);

CREATE  INDEX idx_t1_c2 ON t1 (c2(63));

CREATE  INDEX idx_t1_c6 ON t1 (c6);

CREATE  INDEX idx_t1_c2_c6_c5 ON t1 (c2(63), c6, c5);

CREATE  INDEX idx_t1_c4_c5 ON t1 (c4, c5);

CREATE UNIQUE INDEX idx_t2_pk ON t2 (c1);

CREATE  INDEX idx_t2_c15 ON t2 (c15);

CREATE UNIQUE INDEX idx_t3_pk ON t3 (c1);

CREATE  INDEX idx_t3_c14 ON t3 (c14);

CREATE  INDEX idx_t3_c5_c6 ON t3 (c5, c6);USE test;
CREATE UNIQUE INDEX idx_t1_pk ON t1 (c1);

CREATE  INDEX idx_t1_c3 ON t1 (c3(63));

CREATE  INDEX idx_t1_c6 ON t1 (c6);

CREATE UNIQUE INDEX idx_t2_pk ON t2 (c1);

CREATE  INDEX idx_t2_c13 ON t2 (c13);

CREATE  INDEX idx_t2_c7_c14_c6 ON t2 (c7(50), c14, c6(50));

CREATE UNIQUE INDEX idx_t3_pk ON t3 (c1);

CREATE  INDEX idx_t3_c15 ON t3 (c15);

CREATE  INDEX idx_t3_c15_c7 ON t3 (c15, c7);

CREATE  INDEX idx_t3_c7_c12 ON t3 (c7, c12(50));USE test;
CREATE UNIQUE INDEX idx_t1_pk ON t1 (c1);

CREATE  INDEX idx_t1_c3 ON t1 (c3(63));

CREATE UNIQUE INDEX idx_t2_pk ON t2 (c1);

CREATE  INDEX idx_t2_c15 ON t2 (c15);

CREATE  INDEX idx_t2_c10 ON t2 (c10);

CREATE  INDEX idx_t2_c6_c10 ON t2 (c6(50), c10);

CREATE UNIQUE INDEX idx_t2_pk_c2_c10 ON t2 (c1, c2, c10);

CREATE UNIQUE INDEX idx_t3_pk ON t3 (c1);

CREATE  INDEX idx_t3_c15 ON t3 (c15);

CREATE  INDEX idx_t3_c8 ON t3 (c8);

CREATE  INDEX idx_t3_c2_c3_c11 ON t3 (c2, c3, c11);USE test;
CREATE UNIQUE INDEX idx_t1_pk ON t1 (c1);

CREATE  INDEX idx_t1_c3 ON t1 (c3(63));

CREATE  INDEX idx_t1_c2 ON t1 (c2(63));

CREATE  INDEX idx_t1_c2_c5_c3 ON t1 (c2(63), c5, c3(63));

CREATE UNIQUE INDEX idx_t2_pk ON t2 (c1);

CREATE  INDEX idx_t2_c13 ON t2 (c13);

CREATE  INDEX idx_t2_c10_c4_c12 ON t2 (c10, c4, c12);

CREATE  INDEX idx_t2_c7_c5_c11 ON t2 (c7(50), c5, c11);

CREATE UNIQUE INDEX idx_t2_pk_c6 ON t2 (c1, c6(50));

CREATE UNIQUE INDEX idx_t3_pk ON t3 (c1);

CREATE  INDEX idx_t3_c15 ON t3 (c15);

CREATE  INDEX idx_t3_c11_c15_c9 ON t3 (c11, c15, c9);USE test;
CREATE UNIQUE INDEX idx_t1_pk ON t1 (c1);

CREATE  INDEX idx_t1_c6 ON t1 (c6);

CREATE  INDEX idx_t1_c3_c6_c5 ON t1 (c3(63), c6, c5);

CREATE UNIQUE INDEX idx_t2_pk ON t2 (c1);

CREATE  INDEX idx_t2_c11 ON t2 (c11);

CREATE  INDEX idx_t2_c4_c12 ON t2 (c4, c12);

CREATE  INDEX idx_t2_c14_c13_c6 ON t2 (c14, c13, c6(50));

CREATE UNIQUE INDEX idx_t3_pk ON t3 (c1);

CREATE  INDEX idx_t3_c10 ON t3 (c10(50));

CREATE  INDEX idx_t3_c5 ON t3 (c5);

CREATE  INDEX idx_t3_c5_c12_c2 ON t3 (c5, c12(50), c2);USE test;
CREATE UNIQUE INDEX idx_t1_pk ON t1 (c1);

CREATE  INDEX idx_t1_c5 ON t1 (c5);

CREATE  INDEX idx_t1_c6 ON t1 (c6);

CREATE  INDEX idx_t1_c4_c5 ON t1 (c4, c5);

CREATE UNIQUE INDEX idx_t2_pk ON t2 (c1);

CREATE  INDEX idx_t2_c11 ON t2 (c11);

CREATE  INDEX idx_t2_c13 ON t2 (c13);

CREATE  INDEX idx_t2_c10_c3_c7 ON t2 (c10, c3, c7(50));

CREATE  INDEX idx_t2_c6_c15 ON t2 (c6(50), c15);

CREATE UNIQUE INDEX idx_t2_pk_c3 ON t2 (c1, c3);

CREATE UNIQUE INDEX idx_t3_pk ON t3 (c1);

CREATE  INDEX idx_t3_c11 ON t3 (c11);

CREATE  INDEX idx_t3_c3_c11_c15 ON t3 (c3, c11, c15);USE test;
CREATE UNIQUE INDEX idx_t1_pk ON t1 (c1);

CREATE  INDEX idx_t1_c5 ON t1 (c5);

CREATE  INDEX idx_t1_c2 ON t1 (c2(63));

CREATE  INDEX idx_t1_c6_c2_c5 ON t1 (c6, c2(63), c5);

CREATE  INDEX idx_t1_c6_c4_c3 ON t1 (c6, c4, c3(63));

CREATE UNIQUE INDEX idx_t2_pk ON t2 (c1);

CREATE  INDEX idx_t2_c14 ON t2 (c14);

CREATE  INDEX idx_t2_c11 ON t2 (c11);

CREATE  INDEX idx_t2_c6_c13 ON t2 (c6(50), c13);

CREATE UNIQUE INDEX idx_t3_pk ON t3 (c1);

CREATE  INDEX idx_t3_c8 ON t3 (c8);

CREATE  INDEX idx_t3_c3 ON t3 (c3);

CREATE  INDEX idx_t3_c8_c15 ON t3 (c8, c15);

CREATE  INDEX idx_t3_c7_c12_c15 ON t3 (c7, c12(50), c15);USE test;
CREATE UNIQUE INDEX idx_t1_pk ON t1 (c1);

CREATE  INDEX idx_t1_c4 ON t1 (c4);

CREATE UNIQUE INDEX idx_t2_pk ON t2 (c1);

CREATE  INDEX idx_t2_c5 ON t2 (c5);

CREATE  INDEX idx_t2_c7_c12 ON t2 (c7(50), c12);

CREATE  INDEX idx_t2_c7_c2_c15 ON t2 (c7(50), c2, c15);

CREATE UNIQUE INDEX idx_t3_pk ON t3 (c1);

CREATE  INDEX idx_t3_c9 ON t3 (c9);

CREATE  INDEX idx_t3_c11_c3 ON t3 (c11, c3);USE test;
CREATE UNIQUE INDEX idx_t1_pk ON t1 (c1);

CREATE  INDEX idx_t1_c2 ON t1 (c2(63));

CREATE  INDEX idx_t1_c6_c4_c3 ON t1 (c6, c4, c3(63));

CREATE UNIQUE INDEX idx_t2_pk ON t2 (c1);

CREATE  INDEX idx_t2_c7 ON t2 (c7(50));

CREATE  INDEX idx_t2_c10 ON t2 (c10);

CREATE  INDEX idx_t2_c5_c3 ON t2 (c5, c3);

CREATE UNIQUE INDEX idx_t3_pk ON t3 (c1);

CREATE  INDEX idx_t3_c9 ON t3 (c9);

CREATE  INDEX idx_t3_c2_c9_c14 ON t3 (c2, c9, c14);

CREATE  INDEX idx_t3_c11_c6_c15 ON t3 (c11, c6, c15);

CREATE UNIQUE INDEX idx_t3_pk_c12_c8 ON t3 (c1, c12(50), c8);USE test;
CREATE UNIQUE INDEX idx_t1_pk ON t1 (c1);

CREATE  INDEX idx_t1_c4 ON t1 (c4);

CREATE  INDEX idx_t1_c6 ON t1 (c6);

CREATE  INDEX idx_t1_c6_c2 ON t1 (c6, c2(63));

CREATE UNIQUE INDEX idx_t1_pk_c4 ON t1 (c1, c4);

CREATE UNIQUE INDEX idx_t2_pk ON t2 (c1);

CREATE  INDEX idx_t2_c11 ON t2 (c11);

CREATE UNIQUE INDEX idx_t3_pk ON t3 (c1);

CREATE  INDEX idx_t3_c12 ON t3 (c12(50));

CREATE  INDEX idx_t3_c11_c2 ON t3 (c11, c2);USE test;
CREATE UNIQUE INDEX idx_t1_pk ON t1 (c1);

CREATE  INDEX idx_t1_c3 ON t1 (c3(63));

CREATE  INDEX idx_t1_c2 ON t1 (c2(63));

CREATE  INDEX idx_t1_c4_c2_c3 ON t1 (c4, c2(63), c3(63));

CREATE  INDEX idx_t1_c5_c6 ON t1 (c5, c6);

CREATE UNIQUE INDEX idx_t2_pk ON t2 (c1);

CREATE  INDEX idx_t2_c11 ON t2 (c11);

CREATE  INDEX idx_t2_c2_c12_c7 ON t2 (c2, c12, c7(50));

CREATE  INDEX idx_t2_c10_c14_c12 ON t2 (c10, c14, c12);

CREATE UNIQUE INDEX idx_t3_pk ON t3 (c1);

CREATE  INDEX idx_t3_c2 ON t3 (c2);

CREATE  INDEX idx_t3_c7 ON t3 (c7);

CREATE  INDEX idx_t3_c10_c9 ON t3 (c10(50), c9);

CREATE  INDEX idx_t3_c10_c9 ON t3 (c10(50), c9);

CREATE UNIQUE INDEX idx_t3_pk_c6 ON t3 (c1, c6);USE test;
CREATE UNIQUE INDEX idx_t1_pk ON t1 (c1);

CREATE  INDEX idx_t1_c2 ON t1 (c2(63));

CREATE  INDEX idx_t1_c3 ON t1 (c3(63));

CREATE  INDEX idx_t1_c2_c4_c5 ON t1 (c2(63), c4, c5);

CREATE  INDEX idx_t1_c4_c6_c5 ON t1 (c4, c6, c5);

CREATE UNIQUE INDEX idx_t1_pk_c6 ON t1 (c1, c6);

CREATE UNIQUE INDEX idx_t2_pk ON t2 (c1);

CREATE  INDEX idx_t2_c14 ON t2 (c14);

CREATE  INDEX idx_t2_c12 ON t2 (c12);

CREATE  INDEX idx_t2_c6_c11 ON t2 (c6(50), c11);

CREATE UNIQUE INDEX idx_t3_pk ON t3 (c1);

CREATE  INDEX idx_t3_c9 ON t3 (c9);

CREATE  INDEX idx_t3_c11 ON t3 (c11);

CREATE  INDEX idx_t3_c9_c12_c11 ON t3 (c9, c12(50), c11);

CREATE  INDEX idx_t3_c5_c6_c4 ON t3 (c5, c6, c4);USE test;
CREATE UNIQUE INDEX idx_t1_pk ON t1 (c1);

CREATE  INDEX idx_t1_c4 ON t1 (c4);

CREATE  INDEX idx_t1_c3 ON t1 (c3(63));

CREATE  INDEX idx_t1_c6_c3_c2 ON t1 (c6, c3(63), c2(63));

CREATE  INDEX idx_t1_c2_c6_c5 ON t1 (c2(63), c6, c5);

CREATE UNIQUE INDEX idx_t2_pk ON t2 (c1);

CREATE  INDEX idx_t2_c6 ON t2 (c6(50));

CREATE  INDEX idx_t2_c5_c11_c3 ON t2 (c5, c11, c3);

CREATE  INDEX idx_t2_c11_c10 ON t2 (c11, c10);

CREATE UNIQUE INDEX idx_t3_pk ON t3 (c1);

CREATE  INDEX idx_t3_c10 ON t3 (c10(50));

CREATE  INDEX idx_t3_c9 ON t3 (c9);

CREATE  INDEX idx_t3_c15_c10 ON t3 (c15, c10(50));USE test;
CREATE UNIQUE INDEX idx_t1_pk ON t1 (c1);

CREATE  INDEX idx_t1_c6 ON t1 (c6);

CREATE  INDEX idx_t1_c3_c6 ON t1 (c3(63), c6);

CREATE UNIQUE INDEX idx_t2_pk ON t2 (c1);

CREATE  INDEX idx_t2_c5 ON t2 (c5);

CREATE  INDEX idx_t2_c13_c3 ON t2 (c13, c3);

CREATE UNIQUE INDEX idx_t2_pk_c14 ON t2 (c1, c14);

CREATE UNIQUE INDEX idx_t3_pk ON t3 (c1);

CREATE  INDEX idx_t3_c14 ON t3 (c14);

CREATE  INDEX idx_t3_c9 ON t3 (c9);

CREATE  INDEX idx_t3_c10_c14 ON t3 (c10(50), c14);

CREATE  INDEX idx_t3_c8_c12 ON t3 (c8, c12(50));

CREATE UNIQUE INDEX idx_t3_pk_c3_c14 ON t3 (c1, c3, c14);USE test;
CREATE UNIQUE INDEX idx_t1_pk ON t1 (c1);

CREATE  INDEX idx_t1_c6 ON t1 (c6);

CREATE  INDEX idx_t1_c2_c5 ON t1 (c2(63), c5);

CREATE  INDEX idx_t1_c4_c2_c5 ON t1 (c4, c2(63), c5);

CREATE UNIQUE INDEX idx_t2_pk ON t2 (c1);

CREATE  INDEX idx_t2_c15 ON t2 (c15);

CREATE  INDEX idx_t2_c2 ON t2 (c2);

CREATE  INDEX idx_t2_c7_c2_c14 ON t2 (c7(50), c2, c14);

CREATE UNIQUE INDEX idx_t2_pk_c7 ON t2 (c1, c7(50));

CREATE UNIQUE INDEX idx_t3_pk ON t3 (c1);

CREATE  INDEX idx_t3_c9 ON t3 (c9);

CREATE  INDEX idx_t3_c4 ON t3 (c4);USE test;
CREATE UNIQUE INDEX idx_t1_pk ON t1 (c1);

CREATE  INDEX idx_t1_c3 ON t1 (c3(63));

CREATE  INDEX idx_t1_c4 ON t1 (c4);

CREATE  INDEX idx_t1_c3_c5_c4 ON t1 (c3(63), c5, c4);

CREATE UNIQUE INDEX idx_t2_pk ON t2 (c1);

CREATE  INDEX idx_t2_c4 ON t2 (c4);

CREATE  INDEX idx_t2_c13 ON t2 (c13);

CREATE  INDEX idx_t2_c15_c13 ON t2 (c15, c13);

CREATE UNIQUE INDEX idx_t2_pk_c4_c13 ON t2 (c1, c4, c13);

CREATE UNIQUE INDEX idx_t3_pk ON t3 (c1);

CREATE  INDEX idx_t3_c5 ON t3 (c5);

CREATE  INDEX idx_t3_c2 ON t3 (c2);

CREATE  INDEX idx_t3_c7_c3 ON t3 (c7, c3);USE test;
CREATE UNIQUE INDEX idx_t1_pk ON t1 (c1);

CREATE  INDEX idx_t1_c2 ON t1 (c2(63));

CREATE  INDEX idx_t1_c3_c2 ON t1 (c3(63), c2(63));

CREATE  INDEX idx_t1_c4_c5 ON t1 (c4, c5);

CREATE UNIQUE INDEX idx_t2_pk ON t2 (c1);

CREATE  INDEX idx_t2_c4 ON t2 (c4);

CREATE  INDEX idx_t2_c11 ON t2 (c11);

CREATE  INDEX idx_t2_c13_c2_c10 ON t2 (c13, c2, c10);

CREATE UNIQUE INDEX idx_t2_pk_c6_c3 ON t2 (c1, c6(50), c3);

CREATE UNIQUE INDEX idx_t3_pk ON t3 (c1);

CREATE  INDEX idx_t3_c2 ON t3 (c2);

CREATE  INDEX idx_t3_c9 ON t3 (c9);

CREATE  INDEX idx_t3_c3_c4_c10 ON t3 (c3, c4, c10(50));

CREATE  INDEX idx_t3_c8_c5_c15 ON t3 (c8, c5, c15);USE test;
CREATE UNIQUE INDEX idx_t1_pk ON t1 (c1);

CREATE  INDEX idx_t1_c4 ON t1 (c4);

CREATE  INDEX idx_t1_c4_c2 ON t1 (c4, c2(63));

CREATE UNIQUE INDEX idx_t2_pk ON t2 (c1);

CREATE  INDEX idx_t2_c5 ON t2 (c5);

CREATE  INDEX idx_t2_c11_c6 ON t2 (c11, c6(50));

CREATE  INDEX idx_t2_c15_c14 ON t2 (c15, c14);

CREATE UNIQUE INDEX idx_t2_pk_c10 ON t2 (c1, c10);

CREATE UNIQUE INDEX idx_t3_pk ON t3 (c1);

CREATE  INDEX idx_t3_c2 ON t3 (c2);

CREATE  INDEX idx_t3_c5_c4_c6 ON t3 (c5, c4, c6);

CREATE  INDEX idx_t3_c7_c12_c11 ON t3 (c7, c12(50), c11);

CREATE UNIQUE INDEX idx_t3_pk_c9_c10 ON t3 (c1, c9, c10(50));USE test;
CREATE UNIQUE INDEX idx_t1_pk ON t1 (c1);

CREATE  INDEX idx_t1_c5 ON t1 (c5);

CREATE  INDEX idx_t1_c6 ON t1 (c6);

CREATE  INDEX idx_t1_c4_c6_c2 ON t1 (c4, c6, c2(63));

CREATE UNIQUE INDEX idx_t2_pk ON t2 (c1);

CREATE  INDEX idx_t2_c11 ON t2 (c11);

CREATE  INDEX idx_t2_c3_c7 ON t2 (c3, c7(50));

CREATE  INDEX idx_t2_c2_c3_c13 ON t2 (c2, c3, c13);

CREATE UNIQUE INDEX idx_t3_pk ON t3 (c1);

CREATE  INDEX idx_t3_c7 ON t3 (c7);

CREATE  INDEX idx_t3_c4 ON t3 (c4);

CREATE  INDEX idx_t3_c11_c4 ON t3 (c11, c4);

CREATE  INDEX idx_t3_c6_c14 ON t3 (c6, c14);USE test;
CREATE UNIQUE INDEX idx_t1_pk ON t1 (c1);

CREATE  INDEX idx_t1_c4 ON t1 (c4);

CREATE  INDEX idx_t1_c5_c6 ON t1 (c5, c6);

CREATE UNIQUE INDEX idx_t2_pk ON t2 (c1);

CREATE  INDEX idx_t2_c2 ON t2 (c2);

CREATE UNIQUE INDEX idx_t3_pk ON t3 (c1);

CREATE  INDEX idx_t3_c7 ON t3 (c7);

CREATE  INDEX idx_t3_c5 ON t3 (c5);

CREATE  INDEX idx_t3_c9_c10 ON t3 (c9, c10(50));

CREATE  INDEX idx_t3_c9_c12_c15 ON t3 (c9, c12(50), c15);USE test;
CREATE UNIQUE INDEX idx_t1_pk ON t1 (c1);

CREATE  INDEX idx_t1_c6 ON t1 (c6);

CREATE  INDEX idx_t1_c6_c3 ON t1 (c6, c3(63));

CREATE  INDEX idx_t1_c2_c3 ON t1 (c2(63), c3(63));

CREATE UNIQUE INDEX idx_t2_pk ON t2 (c1);

CREATE  INDEX idx_t2_c4 ON t2 (c4);

CREATE  INDEX idx_t2_c11_c10_c14 ON t2 (c11, c10, c14);

CREATE  INDEX idx_t2_c6_c14_c13 ON t2 (c6(50), c14, c13);

CREATE UNIQUE INDEX idx_t2_pk_c5_c11 ON t2 (c1, c5, c11);

CREATE UNIQUE INDEX idx_t3_pk ON t3 (c1);

CREATE  INDEX idx_t3_c14 ON t3 (c14);

CREATE  INDEX idx_t3_c8_c11_c12 ON t3 (c8, c11, c12(50));

CREATE UNIQUE INDEX idx_t3_pk_c12_c9 ON t3 (c1, c12(50), c9);USE test;
CREATE UNIQUE INDEX idx_t1_pk ON t1 (c1);

CREATE  INDEX idx_t1_c3 ON t1 (c3(63));

CREATE  INDEX idx_t1_c3_c4_c5 ON t1 (c3(63), c4, c5);

CREATE UNIQUE INDEX idx_t1_pk_c5_c2 ON t1 (c1, c5, c2(63));

CREATE UNIQUE INDEX idx_t2_pk ON t2 (c1);

CREATE  INDEX idx_t2_c11 ON t2 (c11);

CREATE  INDEX idx_t2_c12_c13_c15 ON t2 (c12, c13, c15);

CREATE  INDEX idx_t2_c2_c3_c10 ON t2 (c2, c3, c10);

CREATE UNIQUE INDEX idx_t2_pk_c10 ON t2 (c1, c10);

CREATE UNIQUE INDEX idx_t3_pk ON t3 (c1);

CREATE  INDEX idx_t3_c14 ON t3 (c14);

CREATE  INDEX idx_t3_c9_c8_c6 ON t3 (c9, c8, c6);

CREATE UNIQUE INDEX idx_t3_pk_c3 ON t3 (c1, c3);