#pragma once

#include <greptimedb/greptimedb.h>
#include <gtest/gtest.h>

#include <fcntl.h>
#include <poll.h>
#include <spawn.h>
#include <sys/wait.h>
#include <unistd.h>

#include <algorithm>
#include <array>
#include <cerrno>
#include <charconv>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <iostream>
#include <string>
#include <utility>
#include <vector>

extern char **environ;
namespace greptimedb::e2e {
using namespace greptimedb;
namespace proto = greptime::v1;
constexpr std::string_view kContainer = "greptimedb-cxx-e2e";
constexpr std::int64_t kTs = 1700000000000;

inline auto Enabled(const char *key) -> bool {
  const auto *value = std::getenv(key);
  return value && std::string_view(value) == "1";
}
struct CommandResult {
  int code{-1};
  std::string output;
};
/** No shell expansion; every Docker operation has an independent time bound. */
inline auto Docker(std::initializer_list<std::string_view> arguments,
                   std::chrono::seconds budget = std::chrono::seconds{30})
    -> CommandResult {
  std::vector<std::string> owned{
      "timeout", std::to_string(budget.count()) + "s", "docker"};
  for (const auto argument : arguments)
    owned.emplace_back(argument);
  std::vector<char *> argv;
  for (auto &argument : owned)
    argv.push_back(argument.data());
  argv.push_back(nullptr);
  int descriptors[2];
  if (::pipe2(descriptors, O_CLOEXEC) != 0)
    return {};
  posix_spawn_file_actions_t actions;
  if (posix_spawn_file_actions_init(&actions) != 0) {
    ::close(descriptors[0]);
    ::close(descriptors[1]);
    return {};
  }
  const bool configured =
      posix_spawn_file_actions_adddup2(&actions, descriptors[1],
                                       STDOUT_FILENO) == 0 &&
      posix_spawn_file_actions_adddup2(&actions, descriptors[1],
                                       STDERR_FILENO) == 0 &&
      posix_spawn_file_actions_addclose(&actions, descriptors[0]) == 0 &&
      posix_spawn_file_actions_addclose(&actions, descriptors[1]) == 0;
  pid_t child{};
  const int spawned = configured ? posix_spawnp(&child, "timeout", &actions,
                                                nullptr, argv.data(), environ)
                                 : -1;
  posix_spawn_file_actions_destroy(&actions);
  ::close(descriptors[1]);
  CommandResult result;
  if (spawned == 0) {
    std::array<char, 4096> buffer{};
    while (true) {
      const auto count = ::read(descriptors[0], buffer.data(), buffer.size());
      if (count < 0 && errno == EINTR)
        continue;
      if (count <= 0)
        break;
      if (result.output.size() < 1024U * 1024U)
        result.output.append(buffer.data(),
                             std::min(static_cast<std::size_t>(count),
                                      1024U * 1024U - result.output.size()));
    }
    int status{};
    pid_t waited;
    do {
      waited = ::waitpid(child, &status, 0);
    } while (waited < 0 && errno == EINTR);
    if (waited == child && WIFEXITED(status))
      result.code = WEXITSTATUS(status);
  }
  ::close(descriptors[0]);
  return result;
}
inline auto Trim(std::string text) -> std::string {
  const auto first = text.find_first_not_of(" \t\r\n");
  if (first == text.npos)
    return {};
  return text.substr(first, text.find_last_not_of(" \t\r\n") - first + 1);
}
struct Session {
  std::string endpoint, error;
  bool unavailable{}, managed{}, owned{};
  ~Session() {
    if (owned && Enabled("GREPTIMEDB_E2E_REMOVE_CONTAINER")) {
      const auto removed = Docker({"rm", "-f", kContainer});
      if (removed.code != 0)
        std::cerr << "Container cleanup: " << removed.output;
    }
  }
  void Prepare() {
    if (const char *configured = std::getenv("GREPTIMEDB_TEST_ENDPOINT")) {
      endpoint = Trim(configured);
      if (endpoint.empty()) {
        error = "GREPTIMEDB_TEST_ENDPOINT is empty";
        return;
      }
      if (endpoint.find("://") == endpoint.npos)
        endpoint = "grpc://" + endpoint;
      return;
    }
    const auto daemon = Docker({"info", "--format", "{{.ServerVersion}}"});
    if (daemon.code != 0) {
      unavailable = true;
      error = "Docker unavailable and GREPTIMEDB_TEST_ENDPOINT unset: " +
              daemon.output;
      return;
    }
    const auto image =
        Docker({"image", "inspect", "greptime/greptimedb:latest"});
    if (image.code != 0) {
      const auto pulled = Docker({"pull", "greptime/greptimedb:latest"},
                                 std::chrono::seconds{300});
      if (pulled.code != 0) {
        error = "Docker image pull failed: " + pulled.output;
        return;
      }
    }
    auto state = Docker({"inspect", "--format",
                         "{{.Config.Image}}|{{.State.Running}}", kContainer});
    if (state.code != 0) {
      auto diagnostic = state.output;
      std::ranges::transform(
          diagnostic, diagnostic.data(), [](unsigned char ch) {
            return static_cast<char>(ch >= 'A' && ch <= 'Z' ? ch + ('a' - 'A')
                                                            : ch);
          });
      if (diagnostic.find("no such object") == diagnostic.npos &&
          diagnostic.find("no such container") == diagnostic.npos) {
        error = "Docker inspect failed: " + state.output;
        return;
      }
      const auto started =
          Docker({"run", "-d", "--pull=never", "--name", kContainer, "-p",
                  "127.0.0.1::4001", "greptime/greptimedb:latest", "standalone",
                  "start", "--grpc-bind-addr", "0.0.0.0:4001"});
      if (started.code != 0) {
        error = "Docker run failed: " + started.output;
        return;
      }
      owned = true;
    } else {
      if (!state.output.starts_with("greptime/greptimedb:latest|")) {
        error = "Refusing to reuse a container from a different image";
        return;
      }
      if (Trim(state.output).ends_with("|false")) {
        const auto started = Docker({"start", kContainer});
        if (started.code != 0) {
          error = "Docker start failed: " + started.output;
          return;
        }
      } else if (!Trim(state.output).ends_with("|true")) {
        error = "Invalid container state: " + state.output;
        return;
      }
    }
    managed = true;
    const auto published = Docker({"port", kContainer, "4001/tcp"});
    if (published.code != 0) {
      error = "Docker port failed: " + published.output;
      return;
    }
    const auto line = Trim(published.output);
    const auto separator = line.find(':');
    unsigned port{};
    if (!line.starts_with("127.0.0.1:") || separator == line.npos) {
      error = "Container must publish 4001/tcp on loopback: " + line;
      return;
    }
    const auto number = std::string_view(line).substr(separator + 1);
    const auto parsed =
        std::from_chars(number.data(), number.data() + number.size(), port);
    if (parsed.ec != std::errc{} ||
        parsed.ptr != number.data() + number.size() || !port || port > 65535) {
      error = "Invalid Docker port: " + line;
      return;
    }
    endpoint = "grpc://127.0.0.1:" + std::to_string(port);
  }
};

inline auto Connect(std::string_view endpoint, std::string schema = "public",
                    std::chrono::nanoseconds timeout = std::chrono::seconds{10})
    -> Result<std::unique_ptr<Client>> {
  auto parsed = Endpoint::Parse(endpoint);
  if (!parsed)
    return std::unexpected(parsed.error());
  ClientOptions options{std::move(*parsed)};
  options.schema = std::move(schema);
  options.request_timeout = timeout;
  return Client::Connect(std::move(options));
}
inline auto WaitReady(const Session &session)
    -> Result<std::unique_ptr<Client>> {
  const auto deadline =
      std::chrono::steady_clock::now() + std::chrono::seconds{90};
  std::string last;
  do {
    auto client =
        Connect(session.endpoint, "public", std::chrono::milliseconds{500});
    if (client) {
      auto ping = (*client)->Ping();
      if (ping) {
        auto health = (*client)->ProbeHealth();
        if (health && health->healthy)
          return Connect(session.endpoint);
        last = health ? "ProbeHealth reported unhealthy"
                      : std::string(health.error().Message());
      } else
        last = ping.error().Message();
    } else
      last = client.error().Message();
    ::poll(nullptr, 0, 100);
  } while (std::chrono::steady_clock::now() < deadline);
  if (session.managed) {
    auto logs = Docker({"logs", "--tail", "40", kContainer});
    last += "\n" + logs.output;
  }
  return std::unexpected(
      Error::Transport("Health readiness timed out: " + last));
}

struct DatabaseCleanup {
  Client &client;
  std::string schema;
  bool active{true};
  auto Run() -> Result<void> {
    auto query = client.Query("DROP DATABASE " + schema);
    if (!query)
      return std::unexpected(query.error());
    auto finished = (*query)->Finish();
    if (!finished)
      return std::unexpected(finished.error());
    active = false;
    return {};
  }
  ~DatabaseCleanup() {
    if (active) {
      auto removed = Run();
      if (!removed)
        std::cerr << "Database cleanup: " << removed.error().Message() << '\n';
    }
  }
};

} // namespace greptimedb::e2e
