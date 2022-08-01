package utils_test

import (
	"juno-contracts-worker/utils"

	"github.com/stretchr/testify/suite"

	"testing"
)

type Utils struct {
	suite.Suite
}

func (u *Utils) TestShortString() {
	str := "msg_instantiate_contract_42_group_instantiate_newgroup_voter"
	expect := "mic42ginv"

	u.Equal(expect, utils.UniqueShortName(str))
}

func (u *Utils) TestAddUnderscore() {
	u.Equal("code_id", utils.AddUnderscoreIfMissing("code_id"))

	u.Equal("cw_20_id", utils.AddUnderscoreIfMissing("cw20_id"))

	u.Equal("cw_721_code_id", utils.AddUnderscoreIfMissing("cw721_code_id"))

	u.Equal("cw_20", utils.AddUnderscoreIfMissing("cw20"))
}

func (u *Utils) TestGetFieldName() {
	text := "FOREIGN KEY (mic504checkmint) REFERENCES app.mic504checkmint(id)"

	u.Equal("mic504checkmint", utils.GetFieldName(text))
}

func TestUtils(t *testing.T) {
	suite.Run(t, new(Utils))
}
