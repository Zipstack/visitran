import { useEffect, useState } from "react";
import PropTypes from "prop-types";
import { Editor } from "@monaco-editor/react";

import { fetchSQLQuery } from "./SQLServices";
import { useUserStore } from "../../store/user-store";
import { THEME } from "../../common/constants.js";
import { SpinnerLoader } from "../../widgets/spinner_loader/index.js";

function Body({ isSQLDrawerOpen, activeModel }) {
  const [sql, setSql] = useState("");
  const [isLoading, setIsLoading] = useState(true);

  const { currentTheme } = useUserStore((state) => state.userDetails);

  useEffect(() => {
    const loadSQL = async () => {
      if (
        !isSQLDrawerOpen ||
        !activeModel ||
        Object.keys(activeModel).length === 0
      ) {
        setSql("-- No model selected.");
        setIsLoading(false);
        return;
      }

      setIsLoading(true);
      const result = await fetchSQLQuery(activeModel);
      setSql(result?.sql || "");

      setIsLoading(false);
    };

    loadSQL();
  }, [activeModel, isSQLDrawerOpen]);
  return (
    <div className="height-100 width-100">
      <Editor
        value={!isLoading ? sql : "-- Loading..."}
        language="sql"
        height="100%"
        width="100%"
        options={{
          readOnly: true,
          scrollBeyondLastLine: false,
        }}
        theme={currentTheme === THEME.DARK ? "vs-dark" : "light"}
        loading={<SpinnerLoader />}
      />
    </div>
  );
}

Body.propTypes = {
  activeModel: PropTypes.string.isRequired,
  isSQLDrawerOpen: PropTypes.bool.isRequired,
};

export { Body };
