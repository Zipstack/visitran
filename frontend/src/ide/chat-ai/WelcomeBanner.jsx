import { memo, useState, useEffect } from "react";
import PropTypes from "prop-types";
import { Space, Typography } from "antd";

import { useUserStore } from "../../store/user-store";
import { useSessionStore } from "../../store/session-store";
import { VisitranAIDarkIcon, VisitranAILightIcon } from "../../base/icons";

const WelcomeBanner = memo(function WelcomeBanner({
  isOnboardingMode = false,
}) {
  const currentTheme = useUserStore(
    (state) => state?.userDetails?.currentTheme
  );
  const { sessionDetails } = useSessionStore();
  const [greeting, setGreeting] = useState("");
  const [isVisible, setIsVisible] = useState(false);
  const [typedText, setTypedText] = useState("");
  const [isTyping, setIsTyping] = useState(false);

  // Get time-based greeting
  const getTimeBasedGreeting = () => {
    const hour = new Date().getHours();
    if (hour < 12) return "Good Morning";
    if (hour < 17) return "Good Afternoon";
    return "Good Evening";
  };

  // Get username from session
  const getUsername = () => {
    const email = sessionDetails?.email || "";
    if (email) {
      // Extract name from email (before @)
      const name = email.split("@")[0];
      // Capitalize first letter
      return name.charAt(0).toUpperCase() + name.slice(1);
    }
    return "User";
  };

  useEffect(() => {
    const timeGreeting = getTimeBasedGreeting();
    const username = getUsername();
    setGreeting(`${timeGreeting}, ${username}`);

    // Trigger animation after component mounts
    const timer = setTimeout(() => {
      setIsVisible(true);
      // Start typing effect after greeting appears
      setTimeout(() => {
        setIsTyping(true);
      }, 500);
    }, 300);

    return () => clearTimeout(timer);
  }, [sessionDetails]);

  // Typing effect for the question text
  useEffect(() => {
    if (!isTyping) return;

    const fullText = "What transformation would you like to build?";
    let currentIndex = 0;

    const typeTimer = setInterval(() => {
      if (currentIndex <= fullText.length) {
        setTypedText(fullText.slice(0, currentIndex));
        currentIndex++;
      } else {
        clearInterval(typeTimer);
        // Stop typing animation when complete
        setIsTyping(false);
      }
    }, 50); // Typing speed: 50ms per character

    return () => clearInterval(typeTimer);
  }, [isTyping]);

  return (
    <div
      className="chat-ai-welcome-container"
      style={{ display: "flex", justifyContent: "center", width: "100%" }}
    >
      <Space
        direction="vertical"
        size={0}
        style={{ width: "100%", alignItems: "center" }}
      >
        {/* Animated greeting section */}
        <div
          className={`chat-ai-greeting ${isVisible ? "fade-in" : ""} ${
            isOnboardingMode ? "onboarding-move-up" : ""
          }`}
          style={{
            marginTop: "0px",
            textAlign: "center",
            opacity: isVisible ? 1 : 0,
            transform: isVisible ? "translateY(0)" : "translateY(20px)",
            transition: "all 0.8s cubic-bezier(0.4, 0, 0.2, 1)",
            display: "flex",
            flexDirection: "column",
            alignItems: "center",
            justifyContent: "center",
            width: "100%",
          }}
        >
          <div
            style={{
              width: "80px",
              height: "80px",
              margin: "0 auto 0",
              display: "flex",
              alignItems: "center",
              justifyContent: "center",
              animation: isVisible ? "pulse 2s infinite" : "none",
            }}
          >
            {currentTheme === "dark" ? (
              <VisitranAIDarkIcon style={{ width: "60px", height: "60px" }} />
            ) : (
              <VisitranAILightIcon style={{ width: "60px", height: "60px" }} />
            )}
          </div>

          <Typography.Title
            level={3}
            style={{
              margin: "0 0 8px 0",
              color: currentTheme === "dark" ? "#00A6ED" : "#324C99",
            }}
          >
            {greeting}
          </Typography.Title>

          <Typography.Text
            style={{
              fontSize: "16px",
              color: currentTheme === "dark" ? "#90A4B7" : "#092946",
              minHeight: "24px",
              display: "block",
            }}
          >
            {typedText
              .split(" ")
              .map((word, index) => {
                if (word === "transformation" || word === "build") {
                  return (
                    <span
                      key={index}
                      className="animated-word special-word"
                      style={{
                        fontWeight: "bold",
                        color: currentTheme === "dark" ? "#00A6ED" : "#324C99",
                        animation: isVisible
                          ? "glow 2s ease-in-out infinite alternate"
                          : "none",
                      }}
                    >
                      {word}
                    </span>
                  );
                }
                return <span key={index}>{word}</span>;
              })
              .reduce((prev, curr, index) => {
                return prev === null ? [curr] : [...prev, " ", curr];
              }, null)}
            <span
              className="typing-cursor"
              style={{
                opacity: isTyping ? 1 : 0,
                animation: isTyping ? "blink 1s infinite" : "none",
                marginLeft: "2px",
                color: currentTheme === "dark" ? "#00A6ED" : "#324C99",
                transition: "opacity 0.3s ease",
              }}
            >
              |
            </span>
          </Typography.Text>
        </div>
      </Space>

      <style suppressHydrationWarning>{`
        @keyframes pulse {
          0%,
          100% {
            transform: scale(1);
          }
          50% {
            transform: scale(1.05);
          }
        }

        .fade-in {
          animation: fadeInUp 0.8s cubic-bezier(0.4, 0, 0.2, 1) forwards;
        }

        @keyframes fadeInUp {
          from {
            opacity: 0;
            transform: translateY(30px);
          }
          to {
            opacity: 1;
            transform: translateY(0);
          }
        }

        @keyframes glow {
          0% {
            filter: brightness(1) drop-shadow(0 0 5px rgba(102, 126, 234, 0.3));
            transform: scale(1);
          }
          100% {
            filter: brightness(1.2)
              drop-shadow(0 0 15px rgba(102, 126, 234, 0.6));
            transform: scale(1.02);
          }
        }

        .onboarding-move-up {
          animation: moveUp 1s ease-out forwards;
        }

        @keyframes moveUp {
          from {
            transform: translateY(0);
          }
          to {
            transform: translateY(-20px);
          }
        }

        .animated-word {
          display: inline-block;
          transition: all 0.3s ease;
        }

        .transformation-word:hover {
          filter: brightness(1.3) drop-shadow(0 0 20px rgba(102, 126, 234, 0.8));
          transform: scale(1.05);
        }

        .special-word:hover {
          filter: brightness(1.3) drop-shadow(0 0 20px rgba(102, 126, 234, 0.8));
          transform: scale(1.05);
        }

        @keyframes blink {
          0%,
          50% {
            opacity: 1;
          }
          51%,
          100% {
            opacity: 0;
          }
        }
      `}</style>
    </div>
  );
});

WelcomeBanner.propTypes = {
  isOnboardingMode: PropTypes.bool,
};

WelcomeBanner.displayName = "WelcomeBanner";

export { WelcomeBanner };
