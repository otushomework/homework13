#include <iostream>

#define _DEBUG

#include <iostream>
#include <boost/asio.hpp>
#include <boost/bind.hpp>
#include <list>

using boost::asio::ip::tcp;
using TableData = std::map<int, std::string>;
using Command = std::vector<std::string>;

static inline std::string &ltrim(std::string &s) {
    s.erase(s.begin(), std::find_if(s.begin(), s.end(),
                                    std::not1(std::ptr_fun<int, int>(std::isspace))));
    return s;
}

static inline std::string &rtrim(std::string &s) {
    s.erase(std::find_if(s.rbegin(), s.rend(),
                         std::not1(std::ptr_fun<int, int>(std::isspace))).base(), s.end());
    return s;
}

static inline std::string &trim(std::string &s) {
    return ltrim(rtrim(s));
}

auto split(const std::string &str, char d)
{
    Command r;

    std::string::size_type start = 0;
    std::string::size_type stop = str.find_first_of(d);
    while(stop != std::string::npos)
    {
        r.push_back(str.substr(start, stop - start));

        start = stop + 1;
        stop = str.find_first_of(d, start);
    }

    r.push_back(str.substr(start));

    return r;
}

class Table
{
public:
    Table(std::string tableName)
        : m_tableName(tableName)
    {
#ifdef _DEBUG
        std::cout << "Table " << m_tableName << " " << this << std::endl;
#endif
    }

    ~Table()
    {
#ifdef _DEBUG
        std::cout << "~Table " << m_tableName << " " << this << std::endl;
#endif
    }

private:
    TableData m_data;
    std::string m_tableName;
};

class Database
{
    enum CommandCode
    {
        Insert = 0,
        Truncate,
        Intersection,
        SymmetricDifference
    };

public:
    static Database &instance()
    {
        static Database instance;
        return instance;
    }

    void parse(const char *data, std::size_t size, std::function<void(std::string, bool, bool)> resultsHandler)
    {
        std::string fullData(data, size);
        std::istringstream fdStream(fullData);

        std::string line;
        while (std::getline(fdStream, line))
        {
            line = trim(line);

            if (line.size() == 0)
                continue;

            Command command = split(line, ' ');
            if ( command.size() == 0 )
                continue;

            if ( m_commandsMap.find(command.at(0)) == m_commandsMap.end() )
            {
                resultsHandler("Unsupported command", false, true);
                continue;
            }

            CommandCode commandCode = m_commandsMap.at(command.at(0));

            switch (commandCode) {
            case Insert:
            {
                if (command.size() != 4)
                {
                    resultsHandler("Wrong format", false, true);
                    continue;
                }
                break;
            }
            case Truncate:
            {
                if (command.size() != 2)
                {
                    resultsHandler("Wrong format", false, true);
                    continue;
                }
                break;
            }
            case Intersection:
            {
                if (command.size() != 1)
                {
                    resultsHandler("Wrong format", false, true);
                    continue;
                }
                break;
            }
            case SymmetricDifference:
            {
                if (command.size() != 1)
                {
                    resultsHandler("Wrong format", false, true);
                    continue;
                }
                break;
            }
            default:
                break;
            }
        }
    }

private:
    Database()
    {
#ifdef _DEBUG
        std::cout << "Database " << this << std::endl;
#endif
        m_tables.insert(std::pair<std::string, Table>("A", Table("A")));
        m_tables.insert(std::pair<std::string, Table>("B", Table("B")));

        m_commandsMap.insert(std::pair<std::string, CommandCode>("INSERT", Insert));
        m_commandsMap.insert(std::pair<std::string, CommandCode>("TRUNCATE", Truncate));
        m_commandsMap.insert(std::pair<std::string, CommandCode>("INTERSECTION", Intersection));
        m_commandsMap.insert(std::pair<std::string, CommandCode>("SYMMETRIC_DIFFERENCE", SymmetricDifference));
    }

    ~Database()
    {
#ifdef _DEBUG
        std::cout << "~Database " << this << std::endl;
#endif
        m_tables.clear();
    }

    Database(Database const&) = delete;
    Database& operator= (Database const&) = delete;

private:
    std::map<std::string, Table> m_tables;
    std::map<std::string, CommandCode> m_commandsMap;
};

//https://www.boost.org/doc/libs/1_36_0/doc/html/boost_asio/example/echo/async_tcp_echo_server.cpp
class Session
{
public:
    Session(boost::asio::io_service& io_service)
        : m_socket(io_service)
    {
        //m_asyncHandler = async::connect(bulkSize);
    }

    ~Session()
    {
        //async::disconnect(m_asyncHandler);
    }

    tcp::socket& socket()
    {
        return m_socket;
    }

    void start()
    {
        writeGreeting();

        m_socket.async_read_some(boost::asio::buffer(m_data, max_length),
                                 boost::bind(&Session::handleRead, this,
                                             boost::asio::placeholders::error,
                                             boost::asio::placeholders::bytes_transferred));
    }

    void writeGreeting()
    {
        write(std::string(), true, true);
    }

    void write(std::string line, bool finish, bool greeting)
    {
        std::string prefix = greeting ? "> " : "< ";

        boost::asio::async_write(m_socket,
                                 boost::asio::buffer(prefix + line + (greeting ? "" : "\n")),
                                 boost::bind((finish ? &Session::handleWriteFinish : &Session::handleWriteProgress), this,
                                             boost::asio::placeholders::error));
    }

    void handleRead(const boost::system::error_code& error,
                    size_t bytes_transferred)
    {
        if (!error)
        {
            Database::instance().parse(m_data, bytes_transferred, [this](std::string result, bool isOk, bool finish)
            {
                if (!finish)
                {
                    write(result, false, false);
                }
                else
                {
                    if (isOk)
                    {
                        write("OK", true, false);
                    }
                    else
                    {
                        write("ERR " + result, true, false);
                    }
                }

                writeGreeting();
            });
        }
        else
        {
            delete this;
        }
    }

    //for finish write
    void handleWriteFinish(const boost::system::error_code& error)
    {
        if (!error)
        {
            m_socket.async_read_some(boost::asio::buffer(m_data, max_length),
                                     boost::bind(&Session::handleRead, this,
                                                 boost::asio::placeholders::error,
                                                 boost::asio::placeholders::bytes_transferred));
        }
        else
        {
            delete this;
        }
    }

    //for progress writes
    void handleWriteProgress(const boost::system::error_code& error)
    {
        if (error)
            delete this;
    }
private:
    tcp::socket m_socket;
    enum { max_length = 1024 };
    char m_data[max_length];
};

class Server
{
public:
    Server(boost::asio::io_service& io_service, short port)
        : m_io_service(io_service)
        , m_acceptor(io_service, tcp::endpoint(tcp::v4(), port))
    {
        Session* newSession = new Session(m_io_service);
        m_acceptor.async_accept(newSession->socket(),
                                boost::bind(&Server::handleAccept, this, newSession,
                                            boost::asio::placeholders::error));
    }

    void handleAccept(Session* newSession,
                      const boost::system::error_code& error)
    {
        if (!error)
        {
            newSession->start();
            newSession = new Session(m_io_service);
            m_acceptor.async_accept(newSession->socket(),
                                    boost::bind(&Server::handleAccept, this, newSession,
                                                boost::asio::placeholders::error));
        }
        else
        {
            delete newSession;
        }
    }

private:
    boost::asio::io_service& m_io_service;
    tcp::acceptor m_acceptor;
};

int main(int argc, char * argv[]) {
    try
    {
        if (argc != 2)
        {
            std::cerr << "Usage: async_tcp_echo_server <port>\n";
            return 1;
        }

        boost::asio::io_service io_service;

        using namespace std;
        Server s(io_service, std::atoi(argv[1]));

        io_service.run();
    }
    catch (std::exception& e)
    {
        std::cerr << "Exception: " << e.what() << "\n";
    }

    return 0;
}
