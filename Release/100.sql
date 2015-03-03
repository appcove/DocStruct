BEGIN TRANSACTION;

CREATE TABLE "AWS"."Release" (
    "Version" varchar(32) NOT NULL COLLATE "default"
)
WITH (OIDS=FALSE);

ALTER TABLE "AWS"."Release" OWNER TO "TMTManage";

INSERT INTO "AWS"."Release" ("Version") VALUES ('1.0.0');

END

