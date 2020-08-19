package potato.common.cmd

import org.apache.commons.cli
import org.apache.commons.cli._

/**
 * 基于 apache commons cli 构造的命令行基类。
 */
abstract class CommonCliBase {
  private val parser = new GnuParser()
  private val opts = new Options()
  private var cmd: CommandLine = _

  val cliName: String
  val usageHeader: String = null
  val usageFooter: String = null
  val helpWidth: Int = HelpFormatter.DEFAULT_WIDTH
  val strBuffer = new StringBuffer("\n")

  def main(args: Array[String]): Unit = {
    initOptions(opts)
    try {
      cmd = parser.parse(opts, args)
      handleCmd(cmd)
      println(strBuffer.toString)
    } catch {
      case e: Exception =>
        e.printStackTrace()
        println(strBuffer.toString)
        printHelp()
        sys.exit(1)
    }
  }

  /**
   * 缓存输出结果，待程序结束后统一输出。避免中间输出结果与日志穿插问题，提高可读性。
   */
  def console(msg: String): Unit = strBuffer.append(msg).append("\n")

  def printHelp(): Unit = {
    new HelpFormatter().printHelp(helpWidth, cliName, usageHeader, opts, usageFooter, false)
  }

  /**
   * 预处理，添加[[org.apache.commons.cli.Option]]。
   */
  def initOptions(opts: Options): Unit

  /**
   * 根据已解析命令行参数进行处理。
   */
  def handleCmd(cmd: CommandLine): Unit

  /**
   * @param key 查找命令行的key。
   * @param f1  如果key存在，则进行的操作。
   * @param f0  如果key不存在，则进行的操作。
   */
  def handleKey[R](key: String, f1: () => R, f0: () => R = () => null.asInstanceOf[R]): R = {
    if (cmd.hasOption(key)) f1() else f0()
  }

  /**
   * @param key 查找命令行的key。
   * @param f1  如果key存在，则对值进行的操作。
   * @param f0  如果key不存在，则进行的操作。
   */
  def handleValue[R](key: String, f1: String => R, f0: () => R = () => null.asInstanceOf[R]): R = {
    cmd.getOptionValue(key) match {
      case value: String => f1(value)
      case null => f0()
    }
  }

  /**
   * @param key 查找命令行的key。
   * @param f1  如果key存在，则对值进行的操作。
   * @param f0  如果key不存在，则进行的操作。
   */
  def handleValues[R](key: String, f1: Array[String] => R, f0: () => R = () => null.asInstanceOf[R]): R = {
    cmd.getOptionValues(key) match {
      case values: Array[String] => f1(values)
      case null => f0()
    }
  }

  def optBuilder(shortOpt: String = null): NewOptionBuilder = new NewOptionBuilder(shortOpt)

  def groupBuilder(): OptionGroup = new OptionGroup()

  implicit def addable(builder: NewOptionBuilder): AddableOption = new AddableOption(builder)

  implicit def addable(group: OptionGroup): AddableGroup = new AddableGroup(group)

  class AddableOption(builder: NewOptionBuilder) {
    /**
     * 创建Option并添加至参数列表。
     */
    def add(): Unit = opts.addOption(builder.build())
  }

  class AddableGroup(group: OptionGroup) {
    /**
     * 创建Option并添加至参数列表。
     */
    def add(): Unit = {
      opts.addOptionGroup(group)
    }

    def required(required: Boolean = true): AddableGroup = {
      group.setRequired(required)
      this
    }
  }

}

/**
 * 代码取自1.3版本，为了在1.2版本使用1.3版本的新builder。
 */
class NewOptionBuilder {
  /** the name of the option */
  private var opt: String = _

  /** description of the option */
  private var description: String = _

  /** the long representation of the option */
  private var longOpt: String = _

  /** the name of the argument for this option */
  private var argName: String = "arg"

  /** specifies whether this option is required to be present */
  private var _required: Boolean = _

  /** specifies whether the argument value of this Option is optional */
  private var optionalArg: Boolean = _

  /** the number of argument values this option can have */
  private var numberOfArgs: Int = cli.Option.UNINITIALIZED

  //  /** the type of this Option */
  //  private var valueType: Class[String] = classOf[String]

  /** the character that is the value separator */
  private var valueSep: Char = _

  /**
   * Constructs a new <code>Builder</code> with the minimum
   * required parameters for an <code>Option</code> instance.
   *
   * @param opt short representation of the option
   * @throws IllegalArgumentException if there are any non valid Option characters in { @code opt}
   */
  def this(opt: String) {
    this()
    OptionValidator.validateOption(opt)
    this.opt = opt
  }

  /**
   * Sets the display name for the argument value.
   *
   * @param argName the display name for the argument value.
   * @return this builder, to allow method chaining
   */
  def argName(argName: String): NewOptionBuilder = {
    this.argName = argName
    this
  }

  /**
   * Sets the description for this option.
   *
   * @param description the description of the option.
   * @return this builder, to allow method chaining
   */
  def desc(description: String): NewOptionBuilder = {
    this.description = description
    this
  }

  /**
   * Sets the long name of the Option.
   *
   * @param longOpt the long name of the Option
   * @return this builder, to allow method chaining
   */
  def longOpt(longOpt: String): NewOptionBuilder = {
    this.longOpt = longOpt
    this
  }

  /**
   * Sets the number of argument values the Option can take.
   *
   * @param numberOfArgs the number of argument values
   * @return this builder, to allow method chaining
   */
  def numberOfArgs(numberOfArgs: Int): NewOptionBuilder = {
    this.numberOfArgs = numberOfArgs
    this
  }

  /**
   * Sets whether the Option can have an optional argument.
   *
   * @param isOptional specifies whether the Option can have
   *                   an optional argument.
   * @return this builder, to allow method chaining
   */
  def optionalArg(isOptional: Boolean): NewOptionBuilder = {
    this.optionalArg = isOptional
    this
  }

  /**
   * Marks this Option as required.
   *
   * @return this builder, to allow method chaining
   */
  def required: NewOptionBuilder = {
    required(true)
  }

  /**
   * Sets whether the Option is mandatory.
   *
   * @param required specifies whether the Option is mandatory
   * @return this builder, to allow method chaining
   */
  def required(required: Boolean): NewOptionBuilder = {
    this._required = required
    this
  }

  /**
   * The Option will use '=' as a means to separate argument value.
   *
   * @return this builder, to allow method chaining
   */
  def valueSeparator(): NewOptionBuilder = {
    valueSeparator('=')
  }

  /**
   * The Option will use <code>sep</code> as a means to
   * separate argument values.
   * <p>
   * <b>Example:</b>
   * <pre>
   * Option opt = Option.builder("D").hasArgs()
   * .valueSeparator('=')
   * .build();
   * Options options = new Options();
   * options.addOption(opt);
   * String[] args = {"-Dkey=value"};
   * CommandLineParser parser = new DefaultParser();
   * CommandLine line = parser.parse(options, args);
   * String propertyName = line.getOptionValues("D")[0];  // will be "key"
   * String propertyValue = line.getOptionValues("D")[1]; // will be "value"
   * </pre>
   *
   * @param sep The value separator.
   * @return this builder, to allow method chaining
   */
  def valueSeparator(sep: Char): NewOptionBuilder = {
    valueSep = sep
    this
  }

  /**
   * Indicates that the Option will require an argument.
   *
   * @return this builder, to allow method chaining
   */
  def hasArg: NewOptionBuilder = {
    hasArg(true)
  }

  /**
   * Indicates if the Option has an argument or not.
   *
   * @param hasArg specifies whether the Option takes an argument or not
   * @return this builder, to allow method chaining
   */
  def hasArg(hasArg: Boolean): NewOptionBuilder = {
    // set to UNINITIALIZED when no arg is specified to be compatible with OptionBuilder
    numberOfArgs = if (hasArg) 1 else cli.Option.UNINITIALIZED
    this
  }

  /**
   * Indicates that the Option can have unlimited argument values.
   *
   * @return this builder, to allow method chaining
   */
  def hasArgs: NewOptionBuilder = {
    numberOfArgs = cli.Option.UNLIMITED_VALUES
    this
  }

  /**
   * Constructs an Option with the values declared by this [[NewOptionBuilder]].
   *
   * @return the new { @link Option}
   * @throws IllegalArgumentException if neither { @code opt} or { @code longOpt} has been set
   */
  def build(): cli.Option = {
    if (opt == null && longOpt == null) {
      throw new IllegalArgumentException("Either opt or longOpt must be specified")
    }
    val option = new cli.Option(opt, description)
    option.setArgName(argName)
    option.setLongOpt(longOpt)
    option.setArgs(numberOfArgs)
    option.setOptionalArg(optionalArg)
    option.setRequired(_required)
    option.setValueSeparator(valueSep)
    option
  }
}

object OptionValidator {
  /**
   * Validates whether <code>opt</code> is a permissible Option
   * shortOpt.  The rules that specify if the <code>opt</code>
   * is valid are:
   *
   * <ul>
   * <li>a single character <code>opt</code> that is either
   * ' '(special case), '?', '@' or a letter</li>
   * <li>a multi character <code>opt</code> that only contains
   *  letters.</li>
   * </ul>
   * <p>
   * In case [[NewOptionBuilder.opt]] is null no further validation is performed.
   *
   * @param opt The option string to validate, may be null
   * @throws IllegalArgumentException if the Option is not valid.
   */
  def validateOption(opt: String): Unit = {
    // if opt is NULL do not check further
    if (opt == null) {
      return
    }

    // handle the single character opt
    if (opt.length() == 1) {
      val ch = opt.charAt(0)

      if (!isValidOpt(ch)) {
        throw new IllegalArgumentException("Illegal option name '" + ch + "'")
      }
    }

    // handle the multi character opt
    else {
      for (ch <- opt.toCharArray) {
        if (!isValidChar(ch)) {
          throw new IllegalArgumentException("The option '" + opt + "' contains an illegal "
            + "character : '" + ch + "'")
        }
      }
    }
  }

  /**
   * Returns whether the specified character is a valid Option.
   *
   * @param c the option to validate
   * @return true if <code>c</code> is a letter, '?' or '@', otherwise false.
   */
  private def isValidOpt(c: Char): Boolean = {
    isValidChar(c) || c == '?' || c == '@'
  }

  /**
   * Returns whether the specified character is a valid character.
   *
   * @param c the character to validate
   * @return true if <code>c</code> is a letter.
   */
  private def isValidChar(c: Char): Boolean = {
    Character.isJavaIdentifierPart(c)
  }
}
